package db

import (
	"fmt"
	"unsafe"
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

type page struct {
	buf []byte
}

type pageHeader struct {
	pageSize   uint16
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

func (p *page) getRowData(i int) []byte {
	it := p.getItem(i)
	off := it.off()
	size := it.size()
	return p.buf[off : off+size]
}

func (p *page) insertRowData(buf []byte) error {
	hdr := p.header()
	emptySpaceSize := int(hdr.emptyEnd - hdr.emptyStart)
	if emptySpaceSize < len(buf) {
		return fmt.Errorf("no empty space")
	}
	item := makeItem(len(buf), 0, 0)
	p.setItem(int(hdr.itemNum), item)
	hdr.itemNum++
	return nil
}
