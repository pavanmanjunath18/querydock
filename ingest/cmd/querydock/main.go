// Command querydock is the QueryDock CLI. Subcommands:
//
//	collect   collect one or more UTC days of Kubernetes CI results from Prow's
//	          public artifacts into zstd Parquet + a JSON manifest per day
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/pavanmanjunath18/querydock/ingest/internal/collect"
	"github.com/pavanmanjunath18/querydock/ingest/internal/gcs"
	"github.com/pavanmanjunath18/querydock/ingest/internal/kettle"
	"github.com/pavanmanjunath18/querydock/ingest/internal/parquetout"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "collect":
		os.Exit(runCollect(os.Args[2:]))
	case "parse-junit": // conformance testing against Kettle's Python parser
		os.Exit(runParseJUnit(os.Args[2:]))
	default:
		usage()
	}
}

// runParseJUnit prints one JSON line per file: Kettle-rule parse results.
func runParseJUnit(files []string) int {
	enc := json.NewEncoder(os.Stdout)
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		tests, err := kettle.ParseJUnit(data)
		out := map[string]any{"file": f, "tests": tests, "malformed": errors.Is(err, kettle.ErrMalformed), "bad_time": errors.Is(err, kettle.ErrBadTime)}
		if tests == nil {
			out["tests"] = []kettle.Test{}
		}
		_ = enc.Encode(out)
	}
	return 0
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: querydock collect --date YYYY-MM-DD[,YYYY-MM-DD...] [--data-dir DIR] [--rps N] [--concurrency N]")
	os.Exit(2)
}

// Manifest is committed (data/manifests/) so every dataset file is traceable.
type Manifest struct {
	Day           string         `json:"day"`
	Source        string         `json:"source"`
	Rules         string         `json:"rules"`
	Complete      bool           `json:"complete"`
	Provisional   bool           `json:"provisional"`
	CollectedAt   time.Time      `json:"collected_at"`
	DurationSec   float64        `json:"duration_sec"`
	CollectorRev  string         `json:"collector_rev"`
	Params        map[string]any `json:"params"`
	Stats         collect.Stats  `json:"stats"`
	Requests      int64          `json:"requests"`
	Retries       int64          `json:"retries"`
	Throttled     int64          `json:"throttled"`
	NotFound      int64          `json:"not_found"`
	SlowDowns     int64          `json:"slow_downs"`
	DownloadBytes int64          `json:"download_bytes_wire"`
	BodyBytes     int64          `json:"body_bytes_decoded"`
	Parquet       struct {
		File   string `json:"file"`
		Bytes  int64  `json:"bytes"`
		SHA256 string `json:"sha256"`
		Rows   int    `json:"rows"`
	} `json:"parquet"`
}

func vcsRev() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		rev, dirty := "", ""
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.modified":
				if s.Value == "true" {
					dirty = "+dirty"
				}
			}
		}
		if rev != "" {
			return rev + dirty
		}
	}
	return "unknown"
}

func runCollect(args []string) int {
	fs := flag.NewFlagSet("collect", flag.ExitOnError)
	dates := fs.String("date", "", "UTC day(s) to collect, comma-separated YYYY-MM-DD")
	dataDir := fs.String("data-dir", "../data", "repo data/ directory (raw/ is gitignored, manifests/ is committed)")
	rps := fs.Float64("rps", 20, "max requests per second to GCS (shared by all workers)")
	conc := fs.Int("concurrency", 16, "max in-flight HTTP requests")
	workers := fs.Int("workers", 32, "build-level parallelism")
	jobs := fs.String("jobs", "", "optional comma-separated job names (debugging only)")
	allowRecent := fs.Bool("allow-provisional", false, "allow days less than 2 days old (marked provisional)")
	_ = fs.Parse(args)
	if *dates == "" {
		usage()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var only []string
	if *jobs != "" {
		only = strings.Split(*jobs, ",")
	}
	for _, d := range strings.Split(*dates, ",") {
		day, err := time.Parse("2006-01-02", strings.TrimSpace(d))
		if err != nil {
			log.Error("bad --date", "value", d, "err", err)
			return 2
		}
		provisional := time.Since(day.Add(24*time.Hour)) < 48*time.Hour
		if provisional && !*allowRecent {
			log.Error("day is less than 2 days old; late builds may still arrive (use --allow-provisional)", "day", d)
			return 2
		}
		if err := collectDay(ctx, log, day, provisional, *dataDir, *rps, *conc, *workers, only); err != nil {
			log.Error("collect failed", "day", d, "err", err)
			return 1
		}
	}
	return 0
}

func collectDay(ctx context.Context, log *slog.Logger, day time.Time, provisional bool, dataDir string,
	rps float64, conc, workers int, only []string) error {
	client := gcs.New(gcs.Options{RPS: rps, Concurrency: conc})
	t0 := time.Now()
	log.Info("collecting", "day", day.Format("2006-01-02"), "rps", rps, "concurrency", conc)
	name := "kettle-" + day.Format("2006-01-02")
	pq := filepath.Join(dataDir, "raw", name+".parquet")
	out, err := parquetout.NewStream(pq)
	if err != nil {
		return err
	}
	st, err := collect.Day(ctx, client, collect.Options{Day: day, Workers: workers, Jobs: only, Log: log}, out.Add)
	if err != nil {
		out.Abort()
		return err
	}
	size, sum, err := out.Close()
	if err != nil {
		return err
	}
	rows := out.Rows
	m := Manifest{
		Day:           day.Format("2006-01-02"),
		Source:        "gs://kubernetes-ci-logs (logs/ and pr-logs/), public Prow artifacts",
		Rules:         "kubernetes/test-infra kettle make_db.py + make_json.py @ 528e0fbf0b38 (skipped tests not counted)",
		Complete:      st.Failures == 0 && len(only) == 0,
		Provisional:   provisional,
		CollectedAt:   t0.UTC(),
		DurationSec:   time.Since(t0).Seconds(),
		CollectorRev:  vcsRev(),
		Params:        map[string]any{"rps": rps, "concurrency": conc, "workers": workers, "lookback_hours": collect.Lookback.Hours(), "jobs_filter": only},
		Stats:         st,
		Requests:      client.Stats.Requests.Load(),
		Retries:       client.Stats.Retries.Load(),
		Throttled:     client.Stats.Throttled.Load(),
		NotFound:      client.Stats.NotFound.Load(),
		SlowDowns:     client.Stats.SlowDowns.Load(),
		DownloadBytes: client.Stats.WireBytes.Load(),
		BodyBytes:     client.Stats.BodyBytes.Load(),
	}
	m.Parquet.File, m.Parquet.Bytes, m.Parquet.SHA256, m.Parquet.Rows = filepath.Base(pq), size, sum, rows
	mp := filepath.Join(dataDir, "manifests", name+".json")
	if err := os.MkdirAll(filepath.Dir(mp), 0o755); err != nil {
		return err
	}
	js, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(mp, append(js, '\n'), 0o644); err != nil {
		return err
	}
	log.Info("done", "day", m.Day, "builds", rows, "tests", st.Tests, "requests", m.Requests,
		"download_mb", m.DownloadBytes>>20, "parquet_mb", size>>20, "complete", m.Complete, "sec", int(m.DurationSec))
	return nil
}
