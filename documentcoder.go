package ice

import (
	"bytes"
	"encoding/binary"
	"io"
)

const defaultDocumentChunkSize uint32 = 128

type chunkedDocumentCoder struct {
	chunkSize  uint64
	w          io.Writer
	buf        *bytes.Buffer
	metaBuf    []byte
	n          uint64
	bytes      uint64
	compressed []byte
	offsets    []uint64
}

func NewChunkedDocumentCoder(chunkSize uint64, w io.Writer) *chunkedDocumentCoder {
	t := &chunkedDocumentCoder{
		chunkSize: chunkSize,
		w:         w,
	}
	t.buf = bytes.NewBuffer(nil)
	t.metaBuf = make([]byte, binary.MaxVarintLen64)
	t.offsets = append(t.offsets, 0)
	return t
}

func (t *chunkedDocumentCoder) Add(docNum uint64, meta, data []byte) (int, error) {
	var wn, n int
	var err error
	n = binary.PutUvarint(t.metaBuf, uint64(len(meta)))
	if n, err = t.writeToBuf(t.metaBuf[:n]); err != nil {
		return 0, err
	}
	wn += n
	n = binary.PutUvarint(t.metaBuf, uint64(len(data)))
	if n, err = t.writeToBuf(t.metaBuf[:n]); err != nil {
		return 0, err
	}
	wn += n
	if n, err = t.writeToBuf(meta); err != nil {
		return 0, err
	}
	wn += n
	if n, err = t.writeToBuf(data); err != nil {
		return 0, err
	}
	wn += n

	return wn, t.newLine()
}

func (t *chunkedDocumentCoder) writeToBuf(data []byte) (int, error) {
	return t.buf.Write(data)
}

func (t *chunkedDocumentCoder) newLine() error {
	t.n++
	if t.n%t.chunkSize != 0 {
		return nil
	}
	return t.flush()
}

func (t *chunkedDocumentCoder) flush() error {
	if t.buf.Len() > 0 {
		var err error
		t.compressed, err = ZSTDCompress(t.compressed[:cap(t.compressed)], t.buf.Bytes(), 3)
		if err != nil {
			return err
		}
		n, err := t.w.Write(t.compressed)
		if err != nil {
			return err
		}
		t.bytes += uint64(n)
		t.buf.Reset()
	}
	t.offsets = append(t.offsets, t.bytes)
	return nil
}

func (t *chunkedDocumentCoder) Write() error {
	var err error
	var wn, n int
	// flush
	if err = t.flush(); err != nil {
		return err
	}
	// write chunk offsets
	for _, offset := range t.offsets {
		n = binary.PutUvarint(t.metaBuf, offset)
		if _, err = t.w.Write(t.metaBuf[:n]); err != nil {
			return err
		}
		wn += n
	}
	// write chunk offset length
	err = binary.Write(t.w, binary.BigEndian, uint32(wn))
	if err != nil {
		return err
	}
	// write chunk num
	err = binary.Write(t.w, binary.BigEndian, uint32(len(t.offsets)))
	if err != nil {
		return err
	}
	return nil
}

func (t *chunkedDocumentCoder) Reset() {
	t.compressed = t.compressed[:0]
	t.offsets = t.offsets[:0]
	t.n = 0
	t.bytes = 0
	t.buf.Reset()
}

// BufferSize returns buffer len
func (t *chunkedDocumentCoder) BufferSize() uint64 {
	return uint64(t.buf.Len())
}
