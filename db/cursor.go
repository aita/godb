package db

type Cursor struct {
	file     *dbFile
	pageID   pageID
	rowIndex int
}

func (c *Cursor) Next() (bool, error) {
	numPages, err := c.file.numPages()
	if err != nil {
		return false, err
	}
	if numPages <= int(c.pageID) {
		return false, nil
	}
	page, err := c.file.readPage(c.pageID)
	if err != nil {
		return false, err
	}
	hdr := page.header()
	return c.rowIndex < int(hdr.numItems), nil
}

func (c *Cursor) Scan() ([]byte, error) {
	page, err := c.file.readPage(c.pageID)
	if err != nil {
		return nil, err
	}
	src := page.read(c.rowIndex)
	dst := make([]byte, len(src))
	copy(dst, src)
	c.rowIndex++
	hdr := page.header()
	if c.rowIndex >= int(hdr.numItems) {
		c.pageID++
		c.rowIndex = 0
	}
	return dst, nil
}
