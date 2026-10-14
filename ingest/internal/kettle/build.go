package kettle

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
)

// Bucket mirrors one entry of Kettle's buckets.yaml.
type Bucket struct {
	Path        string   // e.g. gs://kubernetes-ci-logs/logs/
	Prefix      string   // prepended to job names
	ExcludeJobs []string // regexes (Python re.findall semantics: match anywhere)
}

// Buckets is kettle/buckets.yaml at the pinned commit. Presubmits under
// gs://kubernetes-ci-logs/pr-logs are always collected with the "pr:" prefix.
var Buckets = []Bucket{{
	Path:   "gs://kubernetes-ci-logs/logs/",
	Prefix: "",
	ExcludeJobs: []string{
		`^ci-test-infra-benchmark-demo$`,
		`^ci-kubernetes-coverage-unit$`,
		`.*(flaky|flake).*`,
	},
}}

const prLogsPath = "gs://kubernetes-ci-logs/pr-logs"

// MaxRowSize is Kettle's per-row cap (BigQuery's limit); larger rows are dropped.
const MaxRowSize = 104857600

// KV is one metadata pair.
type KV struct {
	Key   string `json:"key" parquet:"key"`
	Value string `json:"value" parquet:"value"`
}

// Row is one build, with Kettle's BigQuery field names. Pointer fields are
// omitted (NULL) exactly when Kettle's as_dict() would omit them.
type Row struct {
	Path        string  `json:"path" parquet:"path"`
	Job         string  `json:"job" parquet:"job"`
	Number      *int64  `json:"number,omitempty" parquet:"number,optional"`
	Started     *int64  `json:"started,omitempty" parquet:"started,optional"`
	Finished    *int64  `json:"finished,omitempty" parquet:"finished,optional"`
	Elapsed     *int64  `json:"elapsed,omitempty" parquet:"elapsed,optional"`
	Result      *string `json:"result,omitempty" parquet:"result,optional"`
	Passed      *bool   `json:"passed,omitempty" parquet:"passed,optional"`
	Version     *string `json:"version,omitempty" parquet:"version,optional"`
	Executor    *string `json:"executor,omitempty" parquet:"executor,optional"`
	RepoCommit  *string `json:"repo_commit,omitempty" parquet:"repo_commit,optional"`
	Repos       *string `json:"repos,omitempty" parquet:"repos,optional"`
	Metadata    []KV    `json:"metadata,omitempty" parquet:"metadata,list"`
	Test        []Test  `json:"test" parquet:"test,list"`
	TestsRun    int64   `json:"tests_run" parquet:"tests_run"`
	TestsFailed int64   `json:"tests_failed" parquet:"tests_failed"`
}

// JobAndNumber implements Build.populate_path_to_job_and_number.
func JobAndNumber(buildPath string) (string, *int64, error) {
	if strings.HasSuffix(buildPath, "/") {
		return "", nil, fmt.Errorf("build path must not end in /: %s", buildPath)
	}
	prefix, ok := "", false
	for _, b := range Buckets {
		if strings.HasPrefix(buildPath, b.Path) {
			prefix, ok = b.Prefix, true
			break
		}
	}
	if !ok {
		if !strings.HasPrefix(buildPath, prLogsPath) {
			return "", nil, fmt.Errorf("unknown build path for %s in known bucket paths", buildPath)
		}
		prefix = "pr:"
	}
	job := prefix + path.Base(path.Dir(buildPath))
	if n, err := strconv.ParseInt(path.Base(buildPath), 10, 64); err == nil {
		return job, &n, nil
	}
	return job, nil, nil
}

// pyDumps reproduces Python's json.dumps defaults: ", " and ": " separators,
// insertion-ordered keys and ensure_ascii escaping.
func pyDumps(raw json.RawMessage) (string, error) {
	var b strings.Builder
	if err := writePy(&b, raw); err != nil {
		return "", err
	}
	return b.String(), nil
}

func writePy(b *strings.Builder, raw json.RawMessage) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return errors.New("empty json value")
	}
	switch raw[0] {
	case '{':
		dec := json.NewDecoder(bytes.NewReader(raw))
		if _, err := dec.Token(); err != nil {
			return err
		}
		b.WriteByte('{')
		first := true
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return err
			}
			var v json.RawMessage
			if err := dec.Decode(&v); err != nil {
				return err
			}
			if !first {
				b.WriteString(", ")
			}
			first = false
			writePyString(b, kt.(string))
			b.WriteString(": ")
			if err := writePy(b, v); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case '[':
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return err
		}
		b.WriteByte('[')
		for i, it := range items {
			if i > 0 {
				b.WriteString(", ")
			}
			if err := writePy(b, it); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		writePyString(b, s)
	default: // numbers, true/false/null are already in Python's spelling
		b.Write(raw)
	}
	return nil
}

func writePyString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			switch {
			case r < 0x20 || (r >= 0x7f && r < 0x10000):
				fmt.Fprintf(b, `\u%04x`, r)
			case r >= 0x10000:
				r -= 0x10000
				fmt.Fprintf(b, `\u%04x\u%04x`, 0xd800+(r>>10), 0xdc00+(r&0x3ff))
			default:
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

// obj is a JSON object that remembers key order (Python dicts do).
type obj struct {
	keys []string
	vals map[string]json.RawMessage
}

func parseObj(raw []byte) (*obj, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil, errors.New("not a json object")
	}
	o := &obj{vals: map[string]json.RawMessage{}}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, err
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		k := kt.(string)
		if _, seen := o.vals[k]; !seen {
			o.keys = append(o.keys, k)
		}
		o.vals[k] = v // last duplicate wins, as in Python
	}
	return o, nil
}

func (o *obj) get(k string) (json.RawMessage, bool) {
	if o == nil {
		return nil, false
	}
	v, ok := o.vals[k]
	return v, ok
}

func (o *obj) del(k string) {
	if _, ok := o.vals[k]; !ok {
		return
	}
	delete(o.vals, k)
	for i, key := range o.keys {
		if key == k {
			o.keys = append(o.keys[:i], o.keys[i+1:]...)
			break
		}
	}
}

func isNull(v json.RawMessage) bool { return bytes.Equal(bytes.TrimSpace(v), []byte("null")) }

// pyTruthy reports Python truthiness of a JSON value.
func pyTruthy(v json.RawMessage) bool {
	v = bytes.TrimSpace(v)
	switch {
	case len(v) == 0, isNull(v), bytes.Equal(v, []byte("false")), bytes.Equal(v, []byte(`""`)),
		bytes.Equal(v, []byte("[]")), bytes.Equal(v, []byte("{}")):
		return false
	case v[0] == '{' || v[0] == '[':
		var x any
		_ = json.Unmarshal(v, &x)
		switch t := x.(type) {
		case map[string]any:
			return len(t) > 0
		case []any:
			return len(t) > 0
		}
	case v[0] >= '0' && v[0] <= '9' || v[0] == '-':
		f, err := strconv.ParseFloat(string(v), 64)
		return err != nil || f != 0
	}
	return true
}

func asString(v json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return "", false
	}
	return s, true
}

// pyInt mimics int(x) for a JSON number or numeric string (timestamps).
func pyInt(v json.RawMessage) (int64, error) {
	var n json.Number
	if err := json.Unmarshal(v, &n); err == nil {
		if i, err := n.Int64(); err == nil {
			return i, nil
		}
		if f, err := n.Float64(); err == nil {
			return int64(f), nil // int() truncates toward zero
		}
	}
	if s, ok := asString(v); ok {
		if i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return i, nil
		}
	}
	return 0, fmt.Errorf("int() of %s", string(v))
}

func strPtr(s string) *string { return &s }

// optString returns the string value of key, or nil if absent/null/not a string.
// (Kettle copies whatever JSON value is there; non-string values are not seen in
// Prow's started/finished schema, and BigQuery would reject them anyway.)
func optString(o *obj, k string) *string {
	v, ok := o.get(k)
	if !ok || isNull(v) {
		return nil
	}
	if s, ok := asString(v); ok {
		return &s
	}
	return strPtr(strings.TrimSpace(string(v)))
}

// ErrDropped marks a build Kettle would not emit (make_rows' bare except).
var ErrDropped = errors.New("kettle would drop this build")

// RowForBuild implements make_json.row_for_build. started/finished are the raw
// started.json/finished.json bodies (nil if absent); junits are the raw JUnit files.
// It also reports how many JUnit files were malformed.
func RowForBuild(buildPath string, startedRaw, finishedRaw []byte, junits [][]byte) (*Row, int, error) {
	malformed := 0
	var tests []Test
	for _, j := range junits {
		ts, err := ParseJUnit(j)
		if errors.Is(err, ErrMalformed) {
			malformed++
			continue
		}
		if err != nil {
			return nil, malformed, fmt.Errorf("%w: %v", ErrDropped, err)
		}
		for _, t := range ts {
			if strings.Contains(t.Name, "#") && t.Failed == nil {
				continue // skip successful repeated tests
			}
			tests = append(tests, t)
		}
	}
	started, err := parseObj(startedRaw)
	if err != nil {
		return nil, malformed, fmt.Errorf("%w: started.json: %v", ErrDropped, err)
	}
	finished, err := parseObj(finishedRaw)
	if err != nil {
		return nil, malformed, fmt.Errorf("%w: finished.json: %v", ErrDropped, err)
	}
	if tests == nil {
		tests = []Test{}
	}

	job, number, err := JobAndNumber(buildPath)
	if err != nil {
		return nil, malformed, fmt.Errorf("%w: %v", ErrDropped, err)
	}
	row := &Row{Path: buildPath, Job: job, Number: number, Test: tests, TestsRun: int64(len(tests))}
	for _, t := range tests {
		if t.Failed != nil && *t.Failed {
			row.TestsFailed++
		}
	}

	// get_metadata()
	var metaObj *obj
	if fm, ok := finished.get("metadata"); ok {
		if metaObj, err = parseObj(fm); err != nil {
			return nil, malformed, fmt.Errorf("%w: metadata: %v", ErrDropped, err)
		}
	} else if started != nil {
		if sm, ok := started.get("metadata"); ok {
			if metaObj, err = parseObj(sm); err != nil {
				return nil, malformed, fmt.Errorf("%w: metadata: %v", ErrDropped, err)
			}
		}
	}
	var metaRepos *string
	if metaObj != nil && len(metaObj.keys) > 0 { // `if metadata:` (non-empty dict)
		if v, ok := metaObj.get("repo"); ok && !pyTruthy(v) {
			metaObj.del("repo")
		}
		if finished == nil {
			// finished.get('version', 'N/A') on None raises AttributeError.
			return nil, malformed, fmt.Errorf("%w: metadata without finished.json", ErrDropped)
		}
		buildVersion := json.RawMessage(`"N/A"`)
		if v, ok := finished.get("version"); ok {
			buildVersion = v
		}
		for _, k := range []string{"job-version", "version"} {
			if v, ok := metaObj.get(k); ok && jsonEqual(v, buildVersion) {
				metaObj.del(k)
			}
		}
		pairs := make([]KV, 0, len(metaObj.keys))
		for _, k := range metaObj.keys {
			v := metaObj.vals[k]
			s, isStr := asString(v)
			if !isStr {
				if s, err = pyDumps(v); err != nil {
					return nil, malformed, fmt.Errorf("%w: metadata %s: %v", ErrDropped, k, err)
				}
				if k == "repos" {
					metaRepos = strPtr(s)
				}
			}
			pairs = append(pairs, KV{Key: k, Value: s})
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key })
		row.Metadata = pairs
	}

	// populate_start (`if started:` means non-empty dict)
	if started != nil && len(started.keys) > 0 {
		ts, ok := started.get("timestamp")
		if !ok {
			return nil, malformed, fmt.Errorf("%w: started.json has no timestamp", ErrDropped)
		}
		n, err := pyInt(ts)
		if err != nil {
			return nil, malformed, fmt.Errorf("%w: %v", ErrDropped, err)
		}
		row.Started = &n
		row.Executor = optString(started, "node")
		if v, ok := started.get("repo-commit"); ok {
			row.RepoCommit = jsonOptString(v)
		} else {
			row.RepoCommit = optString(started, "repo-version")
		}
		if v, ok := started.get("repos"); ok && pyTruthy(v) {
			s, err := pyDumps(v)
			if err != nil {
				return nil, malformed, fmt.Errorf("%w: repos: %v", ErrDropped, err)
			}
			row.Repos = &s
		}
	}
	// populate_finish
	if finished != nil && len(finished.keys) > 0 {
		ts, ok := finished.get("timestamp")
		if !ok {
			return nil, malformed, fmt.Errorf("%w: finished.json has no timestamp", ErrDropped)
		}
		n, err := pyInt(ts)
		if err != nil {
			return nil, malformed, fmt.Errorf("%w: %v", ErrDropped, err)
		}
		row.Finished = &n
		row.Version = optString(finished, "version")
		if v, ok := finished.get("result"); ok {
			row.Result = jsonOptString(v)
			p := row.Result != nil && *row.Result == "SUCCESS"
			row.Passed = &p
		} else if v, ok := finished.get("passed"); ok && (string(v) == "true" || string(v) == "false") {
			p := string(v) == "true"
			row.Passed = &p
			if p {
				row.Result = strPtr("SUCCESS")
			} else {
				row.Result = strPtr("FAILURE")
			}
		}
	}
	// populate_meta: self.repos = self.repos if self.repos else repos
	if row.Repos == nil || *row.Repos == "" {
		row.Repos = metaRepos
	}
	// set_elapsed: `if self.started and self.finished` (0 is falsy)
	if row.Started != nil && row.Finished != nil && *row.Started != 0 && *row.Finished != 0 {
		e := *row.Finished - *row.Started
		row.Elapsed = &e
	}
	return row, malformed, nil
}

func jsonOptString(v json.RawMessage) *string {
	if isNull(v) {
		return nil
	}
	if s, ok := asString(v); ok {
		return &s
	}
	return strPtr(strings.TrimSpace(string(v)))
}

func jsonEqual(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xa, _ := json.Marshal(x)
	ya, _ := json.Marshal(y)
	return bytes.Equal(xa, ya)
}

// RowSize approximates Kettle's json_size(row) check (compact JSON is smaller
// than Python's default separators, so this never drops a row Kettle would keep).
func RowSize(r *Row) (int, error) {
	b, err := json.Marshal(r)
	return len(b), err
}
