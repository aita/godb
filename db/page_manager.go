package db

import (
	"container/list"
)

type pageList struct {
	lst      *list.List
	elements map[pageID]*list.Element
}

func newPageList() *pageList {
	l := &pageList{
		lst:      list.New(),
		elements: map[pageID]*list.Element{},
	}
	return l
}

func (l *pageList) len() int {
	return l.lst.Len()
}

func (l *pageList) find(id pageID) *heapPage {
	e, ok := l.elements[id]
	if !ok {
		return nil
	}
	return e.Value.(*heapPage)
}

func (l *pageList) clear() {
	l.lst.Init()
	l.elements = map[pageID]*list.Element{}
}

func (l *pageList) add(page *heapPage) {
	e, ok := l.elements[page.id]
	if !ok {
		e := l.lst.PushFront(page)
		l.elements[page.id] = e
	} else {
		l.lst.MoveToFront(e)
	}
}

func (l *pageList) remove(id pageID) {
	e, ok := l.elements[id]
	if !ok {
		return
	}
	l.lst.Remove(e)
}

func (l *pageList) getLast() *heapPage {
	last := l.lst.Back()
	if last == nil {
		return nil
	}
	return last.Value.(*heapPage)
}

type pageManager struct {
	file    *dbFile
	pages   *pageList
	maxSize int
}

func newPageManager(file *dbFile) *pageManager {
	return &pageManager{
		file:    file,
		pages:   newPageList(),
		maxSize: 1024,
	}
}

func (mgr *pageManager) getPage(id pageID) (*heapPage, error) {
	page := mgr.pages.find(id)
	if page != nil {
		return page, nil
	}
	if mgr.pages.len() >= mgr.maxSize {
		if err := mgr.evictPage(); err != nil {
			return nil, err
		}
	}
	return mgr.file.readPage(id)
}

func (mgr *pageManager) evictPage() error {
	page := mgr.pages.getLast()
	if page == nil {
		return nil
	}
	if err := mgr.flushPage(page); err != nil {
		return err
	}
	mgr.pages.remove(page.id)
	return nil
}

func (mgr *pageManager) flushPage(page *heapPage) error {
	if !page.dirty {
		return nil
	}
	err := mgr.file.writePage(page)
	if err != nil {
		return err
	}
	page.dirty = false
	return nil
}

func (mgr *pageManager) flushAll() error {
	for _, e := range mgr.pages.elements {
		page := e.Value.(*heapPage)
		if err := mgr.flushPage(page); err != nil {
			return err
		}
	}
	return nil
}

func (mgr *pageManager) addPage() (*heapPage, error) {
	if mgr.pages.len() >= mgr.maxSize {
		if err := mgr.evictPage(); err != nil {
			return nil, err
		}
	}
	page, err := mgr.file.addPage()
	if err != nil {
		return nil, err
	}
	mgr.pages.add(page)
	return page, nil
}

func (mgr *pageManager) insertData(data []byte) error {
	numPages, err := mgr.file.numPages()
	if err != nil {
		return err
	}
	for i := 0; i < numPages; i++ {
		page, err := mgr.getPage(pageID(i))
		if err != nil {
			return err
		}
		if page.hasEnoughSpace(data) {
			return page.insert(data)
		}
	}
	page, err := mgr.addPage()
	if err != nil {
		return err
	}
	return page.insert(data)
}
