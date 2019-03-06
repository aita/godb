package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"unsafe"

	"go.uber.org/multierr"
)

const (
	headerMagic uint32 = 0x474f4442 // GODB
	pageSize           = 8 * 1024
)

type dbHeader struct {
	Magic    uint32
	NumPages uint32
}

var (
	dbHeaderSize = int64(unsafe.Sizeof(dbHeader{}))
)

func readHeader(r io.Reader) (hdr dbHeader, err error) {
	err = binary.Read(r, binary.LittleEndian, &hdr)
	return
}

func writeHeader(w io.WriterAt, hdr dbHeader) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, &hdr)
	if err != nil {
		return err
	}
	_, err = w.WriteAt(buf.Bytes(), 0)
	return err
}

type DB struct {
	file  *os.File
	pages []*page
	dirty map[int]*page
}

func (db *DB) Close() error {
	var err error
	if len(db.dirty) > 0 {
		err = db.Sync()
	}
	return multierr.Append(err, db.file.Close())
}

func (db *DB) Sync() error {
	hdr := dbHeader{
		Magic:    headerMagic,
		NumPages: uint32(db.numPages()),
	}
	if err := writeHeader(db.file, hdr); err != nil {
		return err
	}
	for _, page := range db.dirty {
		off := dbHeaderSize + int64(page.id)*pageSize
		_, err := db.file.WriteAt(page.buf, off)
		if err != nil {
			return err
		}
	}
	db.dirty = map[int]*page{}
	return nil
}

func (db *DB) numPages() int {
	return len(db.pages)
}

func (db *DB) Select() [][]byte {
	records := [][]byte{}
	for _, page := range db.pages {
		hdr := page.header()
		for i := 0; i < int(hdr.numItems); i++ {
			src := page.read(i)
			dst := make([]byte, len(src))
			copy(dst, src)
			records = append(records, dst)
		}
	}
	return records
}

func (db *DB) Insert(data []byte) error {
	var p *page
	if len(db.pages) == 0 {
		p = newPage(0, make([]byte, pageSize))
		if err := p.insert(data); err != nil {
			return err
		}
		db.pages = append(db.pages, p)
	} else {
		p = db.pages[len(db.pages)-1]
		err := p.insert(data)
		if err == ErrNoEmptySpace {
			p = newPage(len(db.pages), make([]byte, pageSize))
			if err := p.insert(data); err != nil {
				return err
			}
			db.pages = append(db.pages, p)
		} else if err != nil {
			return err
		}
	}
	db.dirty[p.id] = p
	return nil
}

func Open(path string) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	hdr, err := readHeader(file)
	if err != nil {
		err = multierr.Append(err, file.Close())
		return nil, err
	}
	if hdr.Magic != headerMagic {
		err = errors.New("db: invalid magic number")
		err = multierr.Append(err, file.Close())
		return nil, err
	}
	pages := make([]*page, 0, hdr.NumPages)
	for i := 0; i < int(hdr.NumPages); i++ {
		buf := make([]byte, pageSize)
		if _, err := io.ReadFull(file, buf); err != nil {
			err = multierr.Append(err, file.Close())
			return nil, err
		}
		pages = append(pages, newPage(i, buf))
	}
	db := &DB{
		file:  file,
		pages: pages,
		dirty: map[int]*page{},
	}
	return db, nil
}

func Create(path string) (*DB, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	hdr := dbHeader{
		Magic:    headerMagic,
		NumPages: 0,
	}
	if err = writeHeader(file, hdr); err != nil {
		err = multierr.Append(err, file.Close())
		return nil, err
	}
	db := &DB{
		file:  file,
		pages: []*page{},
	}
	return db, nil
}
