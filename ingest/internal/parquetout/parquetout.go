// Package parquetout writes Kettle-shaped build rows as zstd-compressed Parquet.
package parquetout

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/pavanmanjunath18/querydock/ingest/internal/kettle"
)

// Write atomically writes rows to path and returns the file size and SHA-256.
func Write(path string, rows []kettle.Row) (int64, string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, "", err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, "", err
	}
	w := parquet.NewGenericWriter[kettle.Row](f,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedBetterCompression}),
		parquet.PageBufferSize(1<<20),
		parquet.CreatedBy("querydock-collector", "0.1", ""),
	)
	const chunk = 2000 // one row group per ~2000 builds keeps memory bounded on read
	for i := 0; i < len(rows); i += chunk {
		end := min(i+chunk, len(rows))
		if _, err := w.Write(rows[i:end]); err != nil {
			f.Close()
			return 0, "", fmt.Errorf("parquet write: %w", err)
		}
		if err := w.Flush(); err != nil {
			f.Close()
			return 0, "", err
		}
	}
	if err := w.Close(); err != nil {
		f.Close()
		return 0, "", err
	}
	if err := f.Close(); err != nil {
		return 0, "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, "", err
	}
	return hashFile(path)
}

func hashFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

// Stream writes rows incrementally, flushing a row group every 1000 builds.
type Stream struct {
	path, tmp string
	f         *os.File
	w         *parquet.GenericWriter[kettle.Row]
	buf       []kettle.Row
	Rows      int
}

func NewStream(path string) (*Stream, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &Stream{path: path, tmp: path + ".tmp"}
	f, err := os.Create(s.tmp)
	if err != nil {
		return nil, err
	}
	s.f = f
	s.w = parquet.NewGenericWriter[kettle.Row](f,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedBetterCompression}),
		parquet.CreatedBy("querydock-collector", "0.1", ""))
	return s, nil
}

func (s *Stream) Add(r *kettle.Row) error {
	s.buf = append(s.buf, *r)
	s.Rows++
	if len(s.buf) >= 1000 {
		return s.flush()
	}
	return nil
}

func (s *Stream) flush() error {
	if len(s.buf) == 0 {
		return nil
	}
	if _, err := s.w.Write(s.buf); err != nil {
		return fmt.Errorf("parquet write: %w", err)
	}
	s.buf = s.buf[:0]
	return s.w.Flush()
}

// Close finishes the file atomically and returns its size and SHA-256.
func (s *Stream) Close() (int64, string, error) {
	if err := s.flush(); err != nil {
		s.f.Close()
		return 0, "", err
	}
	if err := s.w.Close(); err != nil {
		s.f.Close()
		return 0, "", err
	}
	if err := s.f.Close(); err != nil {
		return 0, "", err
	}
	if err := os.Rename(s.tmp, s.path); err != nil {
		return 0, "", err
	}
	return hashFile(s.path)
}

// Abort removes the partial file.
func (s *Stream) Abort() { s.f.Close(); os.Remove(s.tmp) }

// Read loads rows back (used by tests and the ingest loader).
func Read(path string) ([]kettle.Row, error) {
	return parquet.ReadFile[kettle.Row](path)
}
