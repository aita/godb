package db

import (
	"errors"
	"unsafe"
)

const (
	pageHeaderSize   = 24
	recordHeaderSize = 4
	itemIDSize       = 4
)

var ErrNoEmptySpace = errors.New("db: no empty space")

type itemID uint32

func makeItemID(size, off int) itemID {
	it := size<<16 | (off & 0xFFFF)
	return itemID(it)
}

func (it itemID) size() int {
	return int(it >> 16)
}

func (it itemID) off() int {
	return int(it & 0xFFFF)
}

type page struct {
	id  int
	buf []byte
}

func newPage(id int, buf []byte) *page {
	p := &page{
		id:  id,
		buf: buf,
	}
	hdr := p.header()
	if hdr.numItems == 0 {
		hdr.emptyStart = pageHeaderSize
		hdr.emptyEnd = uint32(len(buf))
	}
	return p
}

type pageHeader struct {
	numItems   uint16
	emptyStart uint32
	emptyEnd   uint32
}

func (h *pageHeader) remainingSize() int {
	return int(h.emptyEnd - h.emptyStart)
}

func (p *page) header() *pageHeader {
	return (*pageHeader)(unsafe.Pointer(&p.buf[0]))
}

func (p *page) itemPos(i int) uintptr {
	return pageHeaderSize + uintptr(i)*itemIDSize
}

func (p *page) getItem(i int) itemID {
	it := (*itemID)(unsafe.Pointer(&p.buf[p.itemPos(i)]))
	return *it
}

func (p *page) setItem(i int, it itemID) {
	ptr := (*itemID)(unsafe.Pointer(&p.buf[p.itemPos(i)]))
	*ptr = it
}

func (p *page) read(i int) []byte {
	it := p.getItem(i)
	off := it.off()
	size := it.size()
	return p.buf[off : off+size]
}

func (p *page) insert(data []byte) error {
	hdr := p.header()
	size := len(data)
	if hdr.remainingSize() < size {
		return ErrNoEmptySpace
	}
	off := int(hdr.emptyEnd) - size
	it := makeItemID(size, int(hdr.emptyEnd)-size)
	p.setItem(int(hdr.numItems), it)
	copy(p.buf[off:off+size], data)
	hdr.numItems++
	hdr.emptyStart += uint32(itemIDSize)
	hdr.emptyEnd = uint32(off)
	return nil
}
