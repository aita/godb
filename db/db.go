package db

import (
	"encoding/binary"
	"io"
	"os"
	"unsafe"

	"go.uber.org/multierr"
)

const (
	headerMagic = 0x474f4442 // GODB
	pageSize    = 8 * 1024
)

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
	err = multierr.Append(err, db.file.Close())
	return err
}

func (db *DB) Sync() error {
	off := unsafe.Offsetof(dbHeader{}.pageNum)
	buf := make([]byte, unsafe.Sizeof(dbHeader{}.pageNum))
	binary.LittleEndian.PutUint32(buf, uint32(len(db.pages)))
	_, err := db.file.WriteAt(buf, int64(off))
	if err != nil {
		return err
	}
	for _, page := range db.dirty {
		off := unsafe.Sizeof(dbHeader{})
		_, err := db.file.WriteAt(page.buf, int64(off)+int64(page.id)*int64(pageSize))
		if err != nil {
			return err
		}
	}
	db.dirty = map[int]*page{}
	return nil
}

func (db *DB) Select() [][]byte {
	records := [][]byte{}
	for _, page := range db.pages {
		hdr := page.header()
		for i := 0; i < int(hdr.itemNum); i++ {
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

type dbHeader struct {
	magic   uint32
	pageNum uint32
}

func Open(path string) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}

	hdr := dbHeader{}
	buf := make([]byte, unsafe.Sizeof(hdr))
	if _, err := io.ReadFull(file, buf); err != nil {
		err = multierr.Append(err, file.Close())
		return nil, err
	}
	hdr.magic = binary.LittleEndian.Uint32(buf)
	if hdr.magic != headerMagic {
		err = multierr.Append(err, file.Close())
		return nil, err
	}
	hdr.pageNum = binary.LittleEndian.Uint32(buf[unsafe.Offsetof(hdr.pageNum):])
	pages := make([]*page, 0, hdr.pageNum)
	for i := 0; i < int(hdr.pageNum); i++ {
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
		magic:   headerMagic,
		pageNum: 0,
	}
	buf := make([]byte, unsafe.Sizeof(hdr))
	binary.LittleEndian.PutUint32(buf, hdr.magic)
	binary.LittleEndian.PutUint32(buf[unsafe.Offsetof(hdr.pageNum):], hdr.pageNum)
	if _, err := file.Write(buf); err != nil {
		err = multierr.Append(err, file.Close())
		return nil, err
	}
	db := &DB{
		file:  file,
		pages: []*page{},
	}
	return db, nil
}
