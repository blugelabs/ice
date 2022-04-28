package ice

import (
	"bytes"
	"io"

	"github.com/golang/snappy"
)

const zincTrunkerSize = 128

type zincTrunker struct {
	w          io.Writer
	buf        *bytes.Buffer
	n          int
	bytes      int
	compressed []byte
	offsets    []uint64
}

func NewZincTrunker(w io.Writer) *zincTrunker {
	t := &zincTrunker{
		w: w,
	}
	t.buf = bytes.NewBuffer(nil)
	t.offsets = append(t.offsets, 0)
	return t
}
func (t *zincTrunker) Write(data []byte) (int, error) {
	return t.buf.Write(data)
}

func (t *zincTrunker) NewLine() error {
	t.n++
	if t.n%zincTrunkerSize != 0 {
		return nil
	}
	return t.Flush()
}

func (t *zincTrunker) Flush() error {
	if t.buf.Len() > 0 {
		t.compressed = snappy.Encode(t.compressed[:cap(t.compressed)], t.buf.Bytes())
		n, err := t.w.Write(t.compressed)
		if err != nil {
			return err
		}
		t.buf.Reset()
		t.bytes += n
	}
	t.offsets = append(t.offsets, uint64(t.bytes))
	return nil
}

func (t *zincTrunker) Reset() {
	t.compressed = t.compressed[:0]
	t.offsets = t.offsets[:0]
	t.n = 0
	t.bytes = 0
	t.buf.Reset()
}

func (t *zincTrunker) Offsets() []uint64 {
	return t.offsets
}

// Len returns trunk nums
func (t *zincTrunker) Len() int {
	return len(t.offsets)
}

// BufferSize returns buffer len
func (t *zincTrunker) BufferSize() int {
	return t.buf.Len()
}
