package db

import (
	"io"
	"os"
)

type Sink interface {
	io.Closer
	io.WriterAt
	io.ReaderAt

	Size() (int64, error)
}

type fileSink struct {
	*os.File
}

func (sink fileSink) Size() (size int64, err error) {
	stat, err := sink.Stat()
	if err != nil {
		return
	}
	size = stat.Size()
	return
}
