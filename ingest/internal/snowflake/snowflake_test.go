package snowflake

import (
	"testing"
	"time"
)

func TestKnownProwID(t *testing.T) {
	got := Time(2110220480853053440)
	want := time.Date(2026, 10, 14, 4, 5, 43, 293_000_000, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestMinIDRoundTrip(t *testing.T) {
	ts := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	id := MinID(ts)
	if !Time(id).Equal(ts) || Time(id-1).Equal(ts) {
		t.Fatalf("MinID not tight: %d -> %v", id, Time(id))
	}
	if len(itoa(id)) != 19 { // lexicographic listing bounds assume 19-digit IDs
		t.Fatalf("expected 19 digits, got %d", len(itoa(id)))
	}
}

func itoa(v uint64) string {
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
