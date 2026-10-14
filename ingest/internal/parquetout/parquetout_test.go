package parquetout

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pavanmanjunath18/querydock/ingest/internal/kettle"
)

func TestRoundTrip(t *testing.T) {
	yes := true
	txt, ver := "boom", "v1.36.0"
	n, st, fin, el := int64(7), int64(10), int64(15), int64(5)
	rows := []kettle.Row{
		{Path: "gs://kubernetes-ci-logs/logs/j/7", Job: "j", Number: &n, Started: &st, Finished: &fin, Elapsed: &el,
			Version: &ver, Passed: &yes, Metadata: []kettle.KV{{Key: "k", Value: "v"}},
			Test:     []kettle.Test{{Name: "a", Time: 1.5}, {Name: "b", Time: 0, Failed: &yes, FailureText: &txt}},
			TestsRun: 2, TestsFailed: 1},
		{Path: "gs://kubernetes-ci-logs/logs/j/x", Job: "j", Test: []kettle.Test{}},
	}
	p := filepath.Join(t.TempDir(), "d.parquet")
	if _, _, err := Write(p, rows); err != nil {
		t.Fatal(err)
	}
	got, err := Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !reflect.DeepEqual(got[0], rows[0]) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got[0], rows[0])
	}
	if got[1].Number != nil || got[1].Started != nil || len(got[1].Test) != 0 {
		t.Fatalf("nulls not preserved: %+v", got[1])
	}
}
