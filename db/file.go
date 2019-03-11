package db

import (
	"bytes"
	"encoding/binary"
	"os"
	"unsafe"

	"go.uber.org/multierr"
)

const (
	headerMagic     uint32 = 0x42444f47
	defaultPageSize        = 8 * 1024
)

var (
	dbHeaderSize = int(unsafe.Sizeof(dbHeader{}))
)

type dbHeader struct {
	Magic    uint32
	PageSize uint32
}

type dbFile struct {
	sink     Sink
	pageSize int
}

func createFile(path string) (*dbFile, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	df := &dbFile{
		sink:     fileSink{file},
		pageSize: defaultPageSize,
	}
	hdr := dbHeader{
		Magic:    headerMagic,
		PageSize: uint32(df.pageSize),
	}
	if err = df.writeHeader(&hdr); err != nil {
		err = multierr.Append(err, df.close())
		return nil, err
	}
	return df, nil
}

func openFile(path string) (*dbFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	df := &dbFile{
		sink: fileSink{file},
	}
	hdr, err := df.readHeader()
	if err != nil {
		err = multierr.Append(err, df.close())
		return nil, err
	}
	if hdr.Magic != headerMagic {
		err = ErrInvalidMagic
		err = multierr.Append(err, df.close())
		return nil, err
	}
	df.pageSize = int(hdr.PageSize)
	return df, nil
}

func (file *dbFile) close() error {
	return file.sink.Close()
}

func (file *dbFile) readHeader() (hdr dbHeader, err error) {
	buf := make([]byte, dbHeaderSize)
	_, err = file.sink.ReadAt(buf, 0)
	if err != nil {
		return
	}
	r := bytes.NewReader(buf)
	err = binary.Read(r, binary.LittleEndian, &hdr)
	return
}

func (file *dbFile) writeHeader(hdr *dbHeader) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, hdr)
	if err != nil {
		return err
	}
	_, err = file.sink.WriteAt(buf.Bytes(), 0)
	return err
}

func (file *dbFile) readBlock(id pageID) ([]byte, error) {
	off := dbHeaderSize + int(id)*file.pageSize
	buf := make([]byte, file.pageSize)
	if _, err := file.sink.ReadAt(buf, int64(off)); err != nil {
		return nil, err
	}
	return buf, nil
}

func (file *dbFile) writeBlock(id pageID, buf []byte) error {
	off := dbHeaderSize + int(id)*file.pageSize
	_, err := file.sink.WriteAt(buf, int64(off))
	return err
}

func (file *dbFile) numPages() (n int, err error) {
	size, err := file.sink.Size()
	if err != nil {
		return
	}
	size -= int64(dbHeaderSize)
	n = int(size) / file.pageSize
	return
}

func (file *dbFile) readPage(id pageID) (*heapPage, error) {
	buf, err := file.readBlock(id)
	if err != nil {
		return nil, err
	}
	page := newHeapPage(id, buf)
	return page, nil
}

func (file *dbFile) writePage(page *heapPage) error {
	return file.writeBlock(page.id, page.buf)
}

func (file *dbFile) addPage() (*heapPage, error) {
	n, err := file.numPages()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, file.pageSize)
	page := newHeapPage(pageID(n), buf)
	if err := file.writePage(page); err != nil {
		return nil, err
	}
	return page, nil
}
