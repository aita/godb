package db

import (
	"testing"
	"unsafe"

	"gotest.tools/assert"
)

func TestPage(t *testing.T) {
	buf := make([]byte, 8*1024)
	page := newPage(1, buf)

	hdr := page.header()
	assert.Equal(t, uint16(0), hdr.itemNum)
	assert.Equal(t, uint32(pageHeaderSize), hdr.emptyStart)
	assert.Equal(t, uint32(len(buf)), hdr.emptyEnd)

	fixture := [][]byte{
		[]byte("aaa"),
		[]byte("bbbbbbbb"),
		[]byte("cccc"),
	}
	dataLen := 0
	for _, data := range fixture {
		err := page.insert(data)
		if err != nil {
			t.Fatal(err)
		}
		dataLen += len(data)
	}
	for i, data := range fixture {
		assert.DeepEqual(t, data, page.read(i))
	}
	assert.Equal(t, uint16(len(fixture)), hdr.itemNum)
	assert.Equal(t, uint32(pageHeaderSize+int(unsafe.Sizeof(itemID(0)))*len(fixture)), hdr.emptyStart)
	assert.Equal(t, uint32(len(buf)-dataLen), hdr.emptyEnd)
}
