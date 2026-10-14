// Package collect gathers one UTC day of Kubernetes CI results from Prow's public
// artifacts in gs://kubernetes-ci-logs, using the same build discovery as Kettle
// (kettle/make_db.py: logs/ with buckets.yaml exclusions, plus pr-logs via
// pr-logs/directory) and Kettle's row rules (package kettle).
package collect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pavanmanjunath18/querydock/ingest/internal/gcs"
	"github.com/pavanmanjunath18/querydock/ingest/internal/kettle"
	"github.com/pavanmanjunath18/querydock/ingest/internal/snowflake"
)

const bucket = "kubernetes-ci-logs"

// junitRe is make_db.py's _ls_junit_paths filter, applied to top-level artifacts/ entries.
var junitRe = regexp.MustCompile(`.*/junit.*\.xml$`)

// Lookback is how long before the day a build's ID may have been minted and still
// start within the day (queueing). Builds outside this window are not inspected.
const Lookback = 6 * time.Hour

type Options struct {
	Day     time.Time // midnight UTC
	Workers int       // build-level parallelism (requests are still capped by the gcs client)
	Jobs    []string  // optional: restrict to these job names (for tests/debugging)
	Log     *slog.Logger
}

// Stats are written into the day's manifest.
type Stats struct {
	JobsLogs, JobsPR, JobsExcluded            int64
	Candidates, UnparsableIDs                 int64
	BuildsInDay, OutOfDay, Unfinished, NoMeta int64
	Dropped, Oversized                        int64
	DropReasons                               map[string]int64
	JunitFiles, JunitBytes, MalformedJunit    int64
	Tests, TestsFailed                        int64
	Failures                                  int64 // builds we could not fetch completely (day incomplete)
	FailureSamples                            []string
	// StartDelay: seconds from build-ID mint time to the started.json timestamp for
	// in-day builds (p50/p99/p999/max/n). Used to justify the Lookback window.
	StartDelay map[string]int64
}

type counters struct {
	candidates, unparsable, inDay, outOfDay, unfinished, noMeta atomic.Int64
	dropped, oversized, junitFiles, junitBytes, malformed       atomic.Int64
	tests, failed, failures                                     atomic.Int64
	mu                                                          sync.Mutex
	reasons                                                     map[string]int64
	samples                                                     []string
	delays                                                      []int64
}

func (c *counters) fail(msg string) {
	c.failures.Add(1)
	c.mu.Lock()
	if len(c.samples) < 20 {
		c.samples = append(c.samples, msg)
	}
	c.mu.Unlock()
}

func (c *counters) drop(reason string) {
	c.dropped.Add(1)
	c.mu.Lock()
	c.reasons[reason]++
	c.mu.Unlock()
}

type candidate struct {
	pr   bool
	job  string
	id   string
	name string // logs/<job>/<id>/ prefix, or pr-logs/directory/<job>/<id>.txt object
}

// Day collects every finished build whose started.json timestamp falls on o.Day (UTC)
// and passes each row to sink (called from one goroutine at a time).
func Day(ctx context.Context, c *gcs.Client, o Options, sink func(*kettle.Row) error) (Stats, error) {
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	if o.Workers <= 0 {
		o.Workers = 32
	}
	dayStart := o.Day.UTC().Truncate(24 * time.Hour)
	dayEnd := dayStart.Add(24 * time.Hour)
	idLo := strconv.FormatUint(snowflake.MinID(dayStart.Add(-Lookback)), 10)
	idHi := strconv.FormatUint(snowflake.MinID(dayEnd), 10)

	var st Stats
	ct := &counters{reasons: map[string]int64{}}

	// 1. Jobs.
	logJobs, prJobs, excluded, err := listJobs(ctx, c, o.Jobs)
	if err != nil {
		return st, err
	}
	st.JobsLogs, st.JobsPR, st.JobsExcluded = int64(len(logJobs)), int64(len(prJobs)), excluded
	log.Info("jobs", "logs", len(logJobs), "pr", len(prJobs), "excluded", excluded)

	// 2. Candidate builds per job, listing only the ID range that can start on this day.
	cands := make(chan candidate, 4096)
	var listWG sync.WaitGroup
	var listErr atomic.Pointer[error]
	jobCh := make(chan candidate)
	for w := 0; w < o.Workers; w++ {
		listWG.Add(1)
		go func() {
			defer listWG.Done()
			for j := range jobCh {
				if err := listBuilds(ctx, c, j, idLo, idHi, cands, ct); err != nil {
					e := fmt.Errorf("listing %s: %w", j.job, err)
					listErr.CompareAndSwap(nil, &e)
				}
			}
		}()
	}
	go func() {
		for _, j := range logJobs {
			jobCh <- candidate{job: j}
		}
		for _, j := range prJobs {
			jobCh <- candidate{pr: true, job: j}
		}
		close(jobCh)
		listWG.Wait()
		close(cands)
	}()

	// 3. Fetch and build rows.
	var rowsMu sync.Mutex
	var nRows int
	var sinkErr error
	var buildWG sync.WaitGroup
	for w := 0; w < o.Workers; w++ {
		buildWG.Add(1)
		go func() {
			defer buildWG.Done()
			for cd := range cands {
				row, err := fetchBuild(ctx, c, cd, dayStart, dayEnd, ct)
				if err != nil {
					ct.fail(fmt.Sprintf("%s/%s: %v", cd.job, cd.id, err))
					continue
				}
				if row == nil {
					continue
				}
				rowsMu.Lock()
				if sinkErr == nil {
					sinkErr = sink(row)
				}
				nRows++
				n := nRows
				rowsMu.Unlock()
				if n%2000 == 0 {
					log.Info("progress", "builds", n, "requests", c.Stats.Requests.Load(),
						"wire_mb", c.Stats.WireBytes.Load()>>20)
				}
			}
		}()
	}
	buildWG.Wait()
	if p := listErr.Load(); p != nil {
		return st, *p
	}
	if sinkErr != nil {
		return st, sinkErr
	}
	if ctx.Err() != nil {
		return st, ctx.Err()
	}
	st.Candidates, st.UnparsableIDs = ct.candidates.Load(), ct.unparsable.Load()
	st.BuildsInDay, st.OutOfDay, st.Unfinished, st.NoMeta = ct.inDay.Load(), ct.outOfDay.Load(), ct.unfinished.Load(), ct.noMeta.Load()
	st.Dropped, st.Oversized = ct.dropped.Load(), ct.oversized.Load()
	st.DropReasons = ct.reasons
	st.JunitFiles, st.JunitBytes, st.MalformedJunit = ct.junitFiles.Load(), ct.junitBytes.Load(), ct.malformed.Load()
	st.Tests, st.TestsFailed = ct.tests.Load(), ct.failed.Load()
	st.Failures, st.FailureSamples = ct.failures.Load(), ct.samples
	st.StartDelay = delayQuantiles(ct.delays)
	return st, nil
}

func delayQuantiles(d []int64) map[string]int64 {
	if len(d) == 0 {
		return nil
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	q := func(p float64) int64 { return d[min(len(d)-1, int(p*float64(len(d))))] }
	return map[string]int64{"p50": q(0.5), "p99": q(0.99), "p999": q(0.999), "max": d[len(d)-1], "n": int64(len(d))}
}

func listJobs(ctx context.Context, c *gcs.Client, only []string) (logJobs, prJobs []string, excluded int64, err error) {
	var excl []*regexp.Regexp
	for _, p := range kettle.Buckets[0].ExcludeJobs {
		excl = append(excl, regexp.MustCompile(p))
	}
	keep := func(j string) bool {
		if len(only) == 0 {
			return true
		}
		for _, o := range only {
			if o == j {
				return true
			}
		}
		return false
	}
	pre, _, err := c.List(ctx, bucket, gcs.ListOptions{Prefix: "logs/", Delimiter: "/"})
	if err != nil {
		return nil, nil, 0, err
	}
	for _, p := range pre {
		j := strings.TrimSuffix(strings.TrimPrefix(p, "logs/"), "/")
		ex := false
		for _, r := range excl { // re.findall over a lookahead == "matches anywhere"
			if r.MatchString(j) {
				ex = true
				break
			}
		}
		if ex {
			excluded++
			continue
		}
		if keep(j) {
			logJobs = append(logJobs, j)
		}
	}
	pre, _, err = c.List(ctx, bucket, gcs.ListOptions{Prefix: "pr-logs/directory/", Delimiter: "/"})
	if err != nil {
		return nil, nil, 0, err
	}
	for _, p := range pre {
		j := strings.TrimSuffix(strings.TrimPrefix(p, "pr-logs/directory/"), "/")
		if keep(j) { // Kettle applies no exclusions to pr-logs
			prJobs = append(prJobs, j)
		}
	}
	return logJobs, prJobs, excluded, nil
}

func listBuilds(ctx context.Context, c *gcs.Client, j candidate, idLo, idHi string, out chan<- candidate, ct *counters) error {
	if !j.pr {
		base := "logs/" + j.job + "/"
		pre, _, err := c.List(ctx, bucket, gcs.ListOptions{Prefix: base, Delimiter: "/", StartOffset: base + idLo, EndOffset: base + idHi})
		if err != nil {
			return err
		}
		for _, p := range pre {
			id := strings.TrimSuffix(strings.TrimPrefix(p, base), "/")
			if _, err := strconv.ParseUint(id, 10, 64); err != nil {
				ct.unparsable.Add(1)
				continue
			}
			ct.candidates.Add(1)
			out <- candidate{job: j.job, id: id, name: p}
		}
		return nil
	}
	base := "pr-logs/directory/" + j.job + "/"
	_, names, err := c.List(ctx, bucket, gcs.ListOptions{Prefix: base, Delimiter: "/", StartOffset: base + idLo, EndOffset: base + idHi})
	if err != nil {
		return err
	}
	for _, n := range names {
		if !strings.HasSuffix(n, ".txt") || strings.Contains(n, "latest-build") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(n, base), ".txt")
		if _, err := strconv.ParseUint(id, 10, 64); err != nil {
			ct.unparsable.Add(1)
			continue
		}
		ct.candidates.Add(1)
		out <- candidate{pr: true, job: j.job, id: id, name: n}
	}
	return nil
}

func getOptional(ctx context.Context, c *gcs.Client, obj string) ([]byte, error) {
	b, err := c.Get(ctx, bucket, obj)
	if errors.Is(err, gcs.ErrNotFound) {
		return nil, nil
	}
	return b, err
}

// startedAt extracts started.json's timestamp (seconds) for the day filter.
func startedAt(raw []byte) (int64, bool) {
	var v struct {
		Timestamp json.Number `json:"timestamp"`
	}
	if json.Unmarshal(raw, &v) != nil || v.Timestamp == "" {
		return 0, false
	}
	if i, err := v.Timestamp.Int64(); err == nil {
		return i, true
	}
	if f, err := v.Timestamp.Float64(); err == nil {
		return int64(f), true
	}
	return 0, false
}

func fetchBuild(ctx context.Context, c *gcs.Client, cd candidate, dayStart, dayEnd time.Time, ct *counters) (*kettle.Row, error) {
	dir := strings.TrimSuffix(cd.name, "/") // logs/<job>/<id>
	if cd.pr {
		ptr, err := getOptional(ctx, c, cd.name)
		if err != nil {
			return nil, err
		}
		if ptr == nil {
			return nil, nil
		}
		full := strings.TrimSpace(string(ptr)) // gs://kubernetes-ci-logs/pr-logs/pull/...
		dir = strings.TrimPrefix(strings.TrimSuffix(full, "/"), "gs://"+bucket+"/")
	}
	started, err := getOptional(ctx, c, dir+"/started.json")
	if err != nil {
		return nil, err
	}
	if started != nil {
		ts, ok := startedAt(started)
		if ok && (ts < dayStart.Unix() || ts >= dayEnd.Unix()) {
			ct.outOfDay.Add(1)
			return nil, nil
		}
	}
	finished, err := getOptional(ctx, c, dir+"/finished.json")
	if err != nil {
		return nil, err
	}
	if started == nil && finished == nil {
		ct.noMeta.Add(1) // Kettle never inserts such builds
		return nil, nil
	}
	if finished == nil {
		ct.unfinished.Add(1) // Kettle only emits builds with a finished time
		return nil, nil
	}
	if started == nil {
		ct.noMeta.Add(1) // cannot be placed on a day by start time
		return nil, nil
	}

	_, names, err := c.List(ctx, bucket, gcs.ListOptions{Prefix: dir + "/artifacts/", Delimiter: "/"})
	if err != nil {
		return nil, err
	}
	var junits [][]byte
	for _, n := range names {
		if !junitRe.MatchString("gs://" + bucket + "/" + n) {
			continue
		}
		b, err := getOptional(ctx, c, n)
		if err != nil {
			return nil, err
		}
		if b == nil {
			continue
		}
		ct.junitFiles.Add(1)
		ct.junitBytes.Add(int64(len(b)))
		junits = append(junits, b)
	}
	row, malformed, err := kettle.RowForBuild("gs://"+bucket+"/"+dir, started, finished, junits)
	ct.malformed.Add(int64(malformed))
	if err != nil {
		reason := err.Error()
		if i := strings.Index(reason, ": "); i > 0 {
			reason = reason[i+2:]
		}
		if len(reason) > 60 {
			reason = reason[:60]
		}
		ct.drop(reason)
		return nil, nil
	}
	if row.Started == nil { // started.json was an empty object
		ct.noMeta.Add(1)
		return nil, nil
	}
	if size, err := kettle.RowSize(row); err != nil || size > kettle.MaxRowSize {
		ct.oversized.Add(1)
		return nil, nil
	}
	ct.inDay.Add(1)
	if id, err := strconv.ParseUint(cd.id, 10, 64); err == nil {
		ct.mu.Lock()
		ct.delays = append(ct.delays, *row.Started-snowflake.Time(id).Unix())
		ct.mu.Unlock()
	}
	ct.tests.Add(row.TestsRun)
	ct.failed.Add(row.TestsFailed)
	return row, nil
}
