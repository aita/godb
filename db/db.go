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
	headerMagic     uint32 = 0x42444f47
	defaultPageSize        = 8 * 1024
)

type dbHeader struct {
	Magic    uint32
	PageSize uint32
	NumPages uint32
}

var (
	dbHeaderSize = int64(unsafe.Sizeof(dbHeader{}))
)

func readHeader(r io.Reader) (hdr dbHeader, err error) {
	err = binary.Read(r, binary.LittleEndian, &hdr)
	return
}

func writeHeader(w io.WriterAt, hdr *dbHeader) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, hdr)
	if err != nil {
		return err
	}
	_, err = w.WriteAt(buf.Bytes(), 0)
	return err
}

type Sink interface {
	io.ReadWriteCloser
	io.WriterAt
	io.ReaderAt
}

type DB struct {
	sink     Sink
	pageSize int
	pages    []*page
	dirty    map[int]*page
}

func (db *DB) Close() error {
	var err error
	if len(db.dirty) > 0 {
		err = db.Flush()
	}
	return multierr.Append(err, db.sink.Close())
}

func (db *DB) makeHeader() dbHeader {
	return dbHeader{
		Magic:    headerMagic,
		PageSize: uint32(db.pageSize),
		NumPages: uint32(db.numPages()),
	}
}

func (db *DB) Flush() error {
	hdr := db.makeHeader()
	if err := writeHeader(db.sink, &hdr); err != nil {
		return err
	}
	for _, page := range db.dirty {
		if err := db.flushPage(page); err != nil {
			return err
		}
	}
	db.dirty = map[int]*page{}
	return nil
}

func (db *DB) flushPage(p *page) error {
	off := dbHeaderSize + int64(p.id*db.pageSize)
	_, err := db.sink.WriteAt(p.buf, off)
	return err
}

func (db *DB) numPages() int {
	return len(db.pages)
}

type Cursor struct {
	db        *DB
	pageIndex int
	rowIndex  int
}

func (c *Cursor) Next() bool {
	if c.db.numPages() <= c.pageIndex {
		return false
	}
	page := c.db.pages[c.pageIndex]
	hdr := page.header()
	return c.rowIndex < int(hdr.numItems)
}

func (c *Cursor) Scan() []byte {
	page := c.db.pages[c.pageIndex]
	src := page.read(c.rowIndex)
	dst := make([]byte, len(src))
	copy(dst, src)
	c.rowIndex++
	hdr := page.header()
	if c.rowIndex >= int(hdr.numItems) {
		c.pageIndex++
		c.rowIndex = 0
	}
	return dst
}

func (db *DB) Select() *Cursor {
	return &Cursor{
		db:        db,
		pageIndex: 0,
		rowIndex:  0,
	}
}

func (db *DB) addPage() *page {
	p := newPage(0, make([]byte, db.pageSize))
	db.pages = append(db.pages, p)
	return p
}

func (db *DB) Insert(data []byte) error {
	var p *page
	if db.numPages() == 0 {
		p = db.addPage()
	} else {
		p = db.pages[db.numPages()-1]
		if !p.hasEnoughSpace(data) {
			p = db.addPage()
		}
	}
	if err := p.insert(data); err != nil {
		return err
	}
	db.dirty[p.id] = p
	return nil
}

func newDB(sink Sink, pageSize int, pages []*page) *DB {
	return &DB{
		sink:     sink,
		pageSize: pageSize,
		pages:    pages,
		dirty:    map[int]*page{},
	}
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
		buf := make([]byte, hdr.PageSize)
		if _, err := io.ReadFull(file, buf); err != nil {
			err = multierr.Append(err, file.Close())
			return nil, err
		}
		pages = append(pages, newPage(i, buf))
	}
	db := newDB(file, int(hdr.PageSize), pages)
	return db, nil
}

func Create(path string) (*DB, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	hdr := dbHeader{
		Magic:    headerMagic,
		PageSize: defaultPageSize,
		NumPages: 0,
	}
	if err = writeHeader(file, &hdr); err != nil {
		err = multierr.Append(err, file.Close())
		return nil, err
	}
	db := newDB(file, int(hdr.PageSize), nil)
	return db, nil
}
