// Package snowflake decodes Prow build IDs, which are Twitter-style snowflakes:
// milliseconds since 2010-11-04T01:42:54.657Z in the high bits, shifted left 22.
// (Verified against started.json: ID 2110220480853053440 -> 2026-10-14T04:05:43Z.)
package snowflake

import "time"

const epochMillis = 1288834974657

// Time returns the creation time encoded in id.
func Time(id uint64) time.Time {
	return time.UnixMilli(int64(id>>22) + epochMillis).UTC()
}

// MinID returns the smallest ID that could be minted at or after t.
func MinID(t time.Time) uint64 {
	ms := t.UnixMilli() - epochMillis
	if ms < 0 {
		return 0
	}
	return uint64(ms) << 22
}
