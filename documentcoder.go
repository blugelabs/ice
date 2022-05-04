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

func newChunkedDocumentCoder(chunkSize uint64, w io.Writer) *chunkedDocumentCoder {
	c := &chunkedDocumentCoder{
		chunkSize: chunkSize,
		w:         w,
	}
	c.buf = bytes.NewBuffer(nil)
	c.metaBuf = make([]byte, binary.MaxVarintLen64)
	c.offsets = append(c.offsets, 0)
	return c
}

func (c *chunkedDocumentCoder) Add(docNum uint64, meta, data []byte) (int, error) {
	var wn, n int
	var err error
	n = binary.PutUvarint(c.metaBuf, uint64(len(meta)))
	if n, err = c.writeToBuf(c.metaBuf[:n]); err != nil {
		return 0, err
	}
	wn += n
	n = binary.PutUvarint(c.metaBuf, uint64(len(data)))
	if n, err = c.writeToBuf(c.metaBuf[:n]); err != nil {
		return 0, err
	}
	wn += n
	if n, err = c.writeToBuf(meta); err != nil {
		return 0, err
	}
	wn += n
	if n, err = c.writeToBuf(data); err != nil {
		return 0, err
	}
	wn += n

	return wn, c.newLine()
}

func (c *chunkedDocumentCoder) writeToBuf(data []byte) (int, error) {
	return c.buf.Write(data)
}

func (c *chunkedDocumentCoder) newLine() error {
	c.n++
	if c.n%c.chunkSize != 0 {
		return nil
	}
	return c.flush()
}

func (c *chunkedDocumentCoder) flush() error {
	if c.buf.Len() > 0 {
		var err error
		c.compressed, err = ZSTDCompress(c.compressed[:cap(c.compressed)], c.buf.Bytes(), ZSTDCompressionLevel)
		if err != nil {
			return err
		}
		n, err := c.w.Write(c.compressed)
		if err != nil {
			return err
		}
		c.bytes += uint64(n)
		c.buf.Reset()
	}
	c.offsets = append(c.offsets, c.bytes)
	return nil
}

func (c *chunkedDocumentCoder) Write() error {
	var err error
	var wn, n int
	// write chunk offsets
	for _, offset := range c.offsets {
		n = binary.PutUvarint(c.metaBuf, offset)
		if _, err = c.w.Write(c.metaBuf[:n]); err != nil {
			return err
		}
		wn += n
	}
	// write chunk offset length
	err = binary.Write(c.w, binary.BigEndian, uint32(wn))
	if err != nil {
		return err
	}
	// write chunk num
	err = binary.Write(c.w, binary.BigEndian, uint32(len(c.offsets)))
	if err != nil {
		return err
	}
	return nil
}

// Close indicates you are done calling Add() this allows
// the final chunk to be encoded.
func (c *chunkedDocumentCoder) Close() error {
	return c.flush()
}

func (c *chunkedDocumentCoder) Reset() {
	c.compressed = c.compressed[:0]
	c.offsets = c.offsets[:0]
	c.n = 0
	c.bytes = 0
	c.buf.Reset()
}

// BufferSize returns buffer len
func (c *chunkedDocumentCoder) BufferSize() uint64 {
	return uint64(c.buf.Len())
}

// Len returns trunks num
func (c *chunkedDocumentCoder) Len() int {
	return len(c.offsets)
}
