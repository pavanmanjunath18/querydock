package kettle

// Cases ported from kubernetes/test-infra kettle/make_json_test.py (Apache-2.0,
// commit 528e0fbf0b38). testdata/*.xml are copied verbatim from kettle/testdata.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func b(v bool) *bool                   { return &v }
func s(v string) *string               { return &v }
func i(v int64) *int64                 { return &v }
func pass(name string, t float64) Test { return Test{Name: name, Time: t} }
func fail(name string, t float64, text string) Test {
	return Test{Name: name, Time: t, Failed: b(true), FailureText: s(text)}
}

func TestParseJUnitKettleFixtures(t *testing.T) {
	cases := []struct {
		file string
		want []Test
	}{
		{"standard.xml", []Test{
			pass("TearDown Previous", 2.065e-05), pass("Up", 6.342e-06), pass("test setup", 5.298e-06),
			fail("Node Tests", 654.279097299, "error during go run"),
			pass("DumpClusterLogs", 5.399023835), pass("TearDown", 1.21e-05),
			pass("Deferred TearDown", 2.17e-07), pass("Timeout", 3900.0)}},
		{"junit_no_suite_name.xml", []Test{
			fail("<unspecified> TestSimpleFile", 0, "simple_test.go:186: Running tests in file plumbing"),
			pass("<unspecified> TestSimpleFile/plumbing/min/min_program", 0),
			pass("<unspecified> TestSimpleFile/plumbing/eval_results/error_result", 0)}},
		{"failure_message.xml", []Test{
			fail("<unspecified> TestSimpleFile", 0, "simple_test.go:186: Running tests in file plumbing")}},
		{"failure_text.xml", []Test{
			fail("<unspecified> TestSimpleFile", 0, "simple_test.go:186: Running tests in file plumbing")}},
		{"malformed.xml", nil},
		{"malformed_no_testcase_name.xml", []Test{
			pass("TearDown Previous", 2.065e-05), pass("Up", 6.342e-06), pass("test setup", 5.298e-06),
			fail("Node Tests", 654.279097299, "error during go run"),
			pass("<unspecified>", 5.399023835), pass("<unspecified>", 1.21e-05),
			pass("Deferred TearDown", 2.17e-07), pass("Timeout", 3900.0)}},
	}
	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", c.file))
			if err != nil {
				t.Fatal(err)
			}
			got, err := ParseJUnit(data)
			if c.want == nil {
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("want ErrMalformed, got %v (%v)", err, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got  %s\nwant %s", js(got), js(c.want))
			}
		})
	}
}

func TestParseJUnitEdges(t *testing.T) {
	// Both message and body: Kettle joins them with a newline (Ginkgo v2).
	got, _ := ParseJUnit([]byte(`<testsuite><testcase name="a"><failure message="msg">trace</failure></testcase></testsuite>`))
	if want := []Test{fail("a", 0, "msg\ntrace")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %s", js(got))
	}
	// Empty <failure/> still fails, with the default text.
	got, _ = ParseJUnit([]byte(`<testsuite><testcase name="a"><failure/></testcase></testsuite>`))
	if want := []Test{fail("a", 0, "No Failure Message Found")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %s", js(got))
	}
	// <error> is not a failure in Kettle; <skipped> drops the case.
	got, _ = ParseJUnit([]byte(`<testsuite><testcase name="e"><error/></testcase><testcase name="s"><skipped/></testcase></testsuite>`))
	if want := []Test{pass("e", 0)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %s", js(got))
	}
	// Unparsable time drops the build; empty time is 0.
	if _, err := ParseJUnit([]byte(`<testsuite><testcase name="a" time="1,5"/></testsuite>`)); !errors.Is(err, ErrBadTime) {
		t.Fatalf("want ErrBadTime, got %v", err)
	}
	got, _ = ParseJUnit([]byte(`<testsuite><testcase name="a" time=""/></testsuite>`))
	if want := []Test{pass("a", 0)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %s", js(got))
	}
	// Unknown root tag: no tests, not malformed. Invalid UTF-8: malformed.
	if got, err := ParseJUnit([]byte(`<report/>`)); err != nil || len(got) != 0 {
		t.Fatalf("got %v %v", got, err)
	}
	if _, err := ParseJUnit([]byte("<testsuite><testcase name=\"\xff\"/></testsuite>")); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
	// A late <?xml-stylesheet?> PI is legal; a late <?xml ?> declaration is not.
	if _, err := ParseJUnit([]byte(`<?xml version="1.0"?><?xml-stylesheet href="a"?><testsuite/>`)); err != nil {
		t.Fatalf("stylesheet PI rejected: %v", err)
	}
}

func TestJobAndNumber(t *testing.T) {
	for _, c := range []struct {
		path, job string
		num       *int64
	}{
		{"gs://kubernetes-ci-logs/logs/some-build/123", "some-build", i(123)},
		{"gs://kubernetes-ci-logs/logs/some-build/123asdf", "some-build", nil},
		{"gs://kubernetes-ci-logs/pr-logs/123/e2e-node/456", "pr:e2e-node", i(456)},
		{"gs://kubernetes-ci-logs/pr-logs/path", "pr:pr-logs", nil},
	} {
		job, num, err := JobAndNumber(c.path)
		if err != nil || job != c.job || !reflect.DeepEqual(num, c.num) {
			t.Errorf("%s: got %q %v %v", c.path, job, num, err)
		}
	}
	if _, _, err := JobAndNumber("gs://unknown-bucket/foo/123"); err == nil {
		t.Error("unknown bucket accepted")
	}
}

func TestRowForBuild(t *testing.T) {
	path := "gs://kubernetes-ci-logs/logs/J/123"
	base := func() Row { return Row{Path: path, Job: "J", Number: i(123), Test: []Test{}} }

	got, _, err := RowForBuild(path, nil, nil, nil)
	if want := base(); err != nil || !reflect.DeepEqual(*got, want) {
		t.Fatalf("empty: %s %v", js(got), err)
	}

	got, _, _ = RowForBuild(path, []byte(`{"timestamp": 10, "node": "agent-34"}`),
		[]byte(`{"timestamp": 15, "result": "SUCCESS", "version": "v1.2.3"}`), nil)
	want := base()
	want.Started, want.Finished, want.Elapsed = i(10), i(15), i(5)
	want.Version, want.Result, want.Passed, want.Executor = s("v1.2.3"), s("SUCCESS"), b(true), s("agent-34")
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("decorated: %s", js(got))
	}

	got, _, _ = RowForBuild(path, []byte(`{"timestamp": 10}`), []byte(`{"timestamp": 15, "passed": true}`), nil)
	want = base()
	want.Started, want.Finished, want.Elapsed, want.Result, want.Passed = i(10), i(15), i(5), s("SUCCESS"), b(true)
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("passed-only: %s", js(got))
	}

	got, _, _ = RowForBuild(path, nil,
		[]byte(`{"timestamp": 15, "result": "FAILURE", "metadata": {"repo": "ignored", "pull": "asdf"}}`), nil)
	want = base()
	want.Finished, want.Result, want.Passed = i(15), s("FAILURE"), b(false)
	want.Metadata = []KV{{"pull", "asdf"}, {"repo", "ignored"}}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("metadata: %s", js(got))
	}

	junit := `
                   <testsuite>
                    <properties><property name="test" value="don't crash!"></property></properties>
                    <testcase name="t1" time="1.0"><failure>stacktrace</failure></testcase>
                    <testcase name="t2" time="2.0"></testcase>
                    <testcase name="t2#1" time="2.0"></testcase>
                   </testsuite>`
	got, _, _ = RowForBuild(path, nil, nil, [][]byte{[]byte(junit)})
	want = base()
	want.TestsRun, want.TestsFailed = 2, 1
	want.Test = []Test{fail("t1", 1.0, "stacktrace"), pass("t2", 2.0)}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("junit: %s", js(got))
	}
}

func TestPopulateStartFinishKettleCases(t *testing.T) {
	path := "gs://kubernetes-ci-logs/pr-logs/path"
	// "PR Bootstrap" + its finished.json and metadata, from test_gen_build.
	started := `{"node": "0790211c-cacb-11ea-a4b9-4a19d9b965b2", "pull": "master:5a529",
	  "repo-version": "v1.20.0-alpha.0.261+06ea384605f172", "timestamp": 1595278460,
	  "repos": {"k8s.io/kubernetes": "master:5a529", "k8s.io/release": "master"},
	  "version": "v1.20.0-alpha.0.261+06ea384605f172"}`
	finished := `{"timestamp": 1595282312, "version": "v1.20.0-alpha.0.261+06ea384605f172",
	  "result": "SUCCESS", "passed": true, "job-version": "v1.20.0-alpha.0.261+06ea384605f172",
	  "metadata": {"node_os_image": "cos-81-12871-59-0", "infra-commit": "2a9a0f868",
	  "repo": "k8s.io/kubernetes", "master_os_image": "cos-81-12871-59-0",
	  "repos": {"k8s.io/kubernetes": "master:5a529", "k8s.io/release": "master"},
	  "job-version": "v1.20.0-alpha.0.261+06ea384605f172"}}`
	got, _, err := RowForBuild(path, []byte(started), []byte(finished), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := Row{Path: path, Job: "pr:pr-logs", Test: []Test{},
		Executor: s("0790211c-cacb-11ea-a4b9-4a19d9b965b2"), Passed: b(true), Result: s("SUCCESS"),
		Elapsed: i(3852), Started: i(1595278460), Finished: i(1595282312),
		Version:    s("v1.20.0-alpha.0.261+06ea384605f172"),
		RepoCommit: s("v1.20.0-alpha.0.261+06ea384605f172"),
		Repos:      s(`{"k8s.io/kubernetes": "master:5a529", "k8s.io/release": "master"}`),
		Metadata: []KV{
			{"infra-commit", "2a9a0f868"},
			{"master_os_image", "cos-81-12871-59-0"},
			{"node_os_image", "cos-81-12871-59-0"},
			{"repo", "k8s.io/kubernetes"},
			{"repos", `{"k8s.io/kubernetes": "master:5a529", "k8s.io/release": "master"}`},
		}}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("got  %s\nwant %s", js(got), js(want))
	}

	// "CI Decorated": repo-commit wins over repo-version; repos JSON uses Python spacing.
	got, _, _ = RowForBuild(path, []byte(`{"timestamp":1595284709,"repos":{"kubernetes/kubernetes":"master"},
	  "repo-version":"x","repo-commit":"5a529aa3a0dd3a050c5302329681e871ef6c162e"}`), nil, nil)
	if *got.RepoCommit != "5a529aa3a0dd3a050c5302329681e871ef6c162e" || *got.Repos != `{"kubernetes/kubernetes": "master"}` {
		t.Fatalf("ci decorated: %s", js(got))
	}

	// Metadata from started.json with no finished.json: Kettle crashes, so the build is dropped.
	if _, _, err := RowForBuild(path, []byte(`{"timestamp": 1, "metadata": {"a": "b"}}`), nil, nil); !errors.Is(err, ErrDropped) {
		t.Fatalf("want ErrDropped, got %v", err)
	}
}

func TestPyDumps(t *testing.T) {
	got, err := pyDumps(json.RawMessage("{\"b\":[1,true,null],\"a\":\"\u00e9\\\"x\"}"))
	want := "{\"b\": [1, true, null], \"a\": \"\\u00e9\\\"x\"}"
	if err != nil || got != want {
		t.Fatalf("got %s want %s (err %v)", got, want, err)
	}
}

func js(v any) string { out, _ := json.Marshal(v); return string(out) }
