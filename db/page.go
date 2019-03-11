package db

import (
	"unsafe"
)

var (
	pageHeaderSize = int(unsafe.Sizeof(pageHeader{}))
	itemIDSize     = int(unsafe.Sizeof(itemID(0)))
)

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

type pageID int64

type heapPage struct {
	id    pageID
	buf   []byte
	dirty bool
}

func newHeapPage(id pageID, buf []byte) *heapPage {
	page := &heapPage{
		id:    id,
		buf:   buf,
		dirty: false,
	}
	hdr := page.header()
	if hdr.numItems == 0 {
		hdr.emptyStart = uint32(pageHeaderSize)
		hdr.emptyEnd = uint32(len(buf))
	}
	return page
}

type pageHeader struct {
	numItems   uint16
	emptyStart uint32
	emptyEnd   uint32
}

func (h *pageHeader) remainingSize() int {
	return int(h.emptyEnd - h.emptyStart)
}

func (p *heapPage) header() *pageHeader {
	return (*pageHeader)(unsafe.Pointer(&p.buf[0]))
}

func (p *heapPage) itemPos(i int) uintptr {
	return uintptr(pageHeaderSize + i*itemIDSize)
}

func (p *heapPage) getItem(i int) itemID {
	it := (*itemID)(unsafe.Pointer(&p.buf[p.itemPos(i)]))
	return *it
}

func (p *heapPage) setItem(i int, it itemID) {
	ptr := (*itemID)(unsafe.Pointer(&p.buf[p.itemPos(i)]))
	*ptr = it
}

func (p *heapPage) read(i int) []byte {
	it := p.getItem(i)
	off := it.off()
	size := it.size()
	return p.buf[off : off+size]
}

func (p *heapPage) hasEnoughSpace(data []byte) bool {
	hdr := p.header()
	size := len(data)
	return size <= hdr.remainingSize()
}

func (p *heapPage) insert(data []byte) error {
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
	p.dirty = true
	return nil
}
