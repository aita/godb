package db

import (
	"fmt"
	"unsafe"
)

const (
	pageHeaderSize   = 24
	recordHeaderSize = 4
)

type item uint16

func makeItem(size, off, z int) item {
	return item(0)
}

func (it item) size() int {
	return 0
}

func (it item) off() int {
	return 0
}

type record struct {
	key   []byte
	value []byte
}

type page struct {
	buf []byte
}

func newPage(buf []byte) *page {
	p := &page{
		buf: buf,
	}
	hdr := p.header()
	hdr.itemNum = 0
	hdr.emptyStart = pageHeaderSize
	hdr.emptyEnd = uint32(len(buf))
	return p
}

type pageHeader struct {
	itemNum    uint16
	emptyStart uint32
	emptyEnd   uint32
}

func (p *page) header() *pageHeader {
	return (*pageHeader)(unsafe.Pointer(&p.buf[0]))
}

func (p *page) headerPos() uintptr {
	return uintptr(unsafe.Pointer(&p.buf[0]))
}

func (p *page) itemPos(i int) uintptr {
	return p.headerPos() + pageHeaderSize + uintptr(4*i)
}

func (p *page) getItem(i int) item {
	return *(*item)(unsafe.Pointer(p.itemPos(i)))
}

func (p *page) setItem(i int, it item) {
	ptr := (*item)(unsafe.Pointer(p.itemPos(i)))
	*ptr = it
}

func (p *page) read(i int) *record {
	it := p.getItem(i)
	off := it.off()
	size := it.size()
	keySize := int(*(*uint16)(unsafe.Pointer(&p.buf[off])))
	valueSize := int(*(*uint16)(unsafe.Pointer(&p.buf[off+2])))
	keyStart := recordHeaderSize + off
	valueStart := keyStart + keySize
	return &record{
		key:   p.buf[keyStart:keySize],
		value: p.buf[valueStart : valueStart+valueSize],
	}
}

func (p *page) insert(rec record) error {
	hdr := p.header()
	size := recordHeaderSize + len(rec.key) + len(rec.value)
	remainingSize := int(hdr.emptyEnd - hdr.emptyStart)
	if remainingSize < size {
		return fmt.Errorf("no empty space")
	}
	item := makeItem(size, 0, 0)
	p.setItem(int(hdr.itemNum), item)
	off := int(hdr.emptyEnd) - int(size)
	keySize := (*uint16)(unsafe.Pointer(&p.buf[off]))
	valueSize := (*uint16)(unsafe.Pointer(&p.buf[off+2]))
	*keySize = uint16(len(rec.key))
	*valueSize = uint16(len(rec.value))
	copy(p.buf[recordHeaderSize+off:], rec.key)
	copy(p.buf[recordHeaderSize+off+len(rec.key):], rec.value)
	hdr.itemNum++
	return nil
}
