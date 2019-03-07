package db

import (
	"fmt"
	"unsafe"
)

const (
	pageHeaderSize   = 24
	recordHeaderSize = 4
)

type item uint32

func makeItem(size, off int) item {
	it := size<<16 | (off & 0xFFFF)
	return item(it)
}

func (it item) size() int {
	return int(it >> 16)
}

func (it item) off() int {
	return int(it & 0xFFFF)
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

func (p *page) itemPos(i int) uintptr {
	return pageHeaderSize + uintptr(i)*unsafe.Sizeof(item(0))
}

func (p *page) getItem(i int) item {
	it := (*item)(unsafe.Pointer(&p.buf[p.itemPos(i)]))
	return *it
}

func (p *page) setItem(i int, it item) {
	ptr := (*item)(unsafe.Pointer(&p.buf[p.itemPos(i)]))
	*ptr = it
}

func (p *page) read(i int) *record {
	it := p.getItem(i)
	off := it.off()
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
	it := makeItem(size, int(hdr.emptyEnd)-size)
	p.setItem(int(hdr.itemNum), it)
	off := int(hdr.emptyEnd) - int(size)
	keySize := (*uint16)(unsafe.Pointer(&p.buf[off]))
	valueSize := (*uint16)(unsafe.Pointer(&p.buf[off+2]))
	*keySize = uint16(len(rec.key))
	*valueSize = uint16(len(rec.value))
	copy(p.buf[recordHeaderSize+off:], rec.key)
	copy(p.buf[recordHeaderSize+off+len(rec.key):], rec.value)
	hdr.itemNum++
	hdr.emptyStart += uint32(unsafe.Sizeof(it))
	hdr.emptyEnd -= uint32(size)
	return nil
}
