package db

import (
	"os"
)

const (
	headerMagic = 0x474f4442 // GODB
	pageSize    = 8 * 1024   // 8KiB
)

type Table struct {
	file *os.File

	pageSize uint32
	pageNum  uint32
	pages    []page
}

func Open(path string) (*Table, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return OpenFile(file)
}

func OpenFile(file *os.File) (*Table, error) {
	tbl := &Table{
		file: file,
	}
	return tbl, nil
}

func Create(path string) (*Table, error) {
	return
}
