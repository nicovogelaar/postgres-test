package weather

import (
	"io"
)

type countReader struct {
	reader    io.Reader
	bytesRead int64
}

func newCountReader(reader io.Reader) *countReader {
	return &countReader{reader: reader}
}

func (r *countReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.bytesRead += int64(n)
	return n, err
}
