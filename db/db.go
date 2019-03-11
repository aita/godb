package db

import (
	"go.uber.org/multierr"
)

type DB struct {
	file        *dbFile
	pageManager *pageManager
}

func (db *DB) Close() error {
	err := db.Flush()
	return multierr.Append(err, db.file.close())
}

func (db *DB) Flush() error {
	return db.pageManager.flushAll()
}

func (db *DB) Select() *Cursor {
	return &Cursor{
		file:     db.file,
		pageID:   0,
		rowIndex: 0,
	}
}

func (db *DB) Insert(data []byte) error {
	return db.pageManager.insertData(data)
}

func Open(path string) (*DB, error) {
	file, err := openFile(path)
	if err != nil {
		return nil, err
	}
	db := &DB{
		file:        file,
		pageManager: newPageManager(file),
	}
	return db, nil
}

func Create(path string) (*DB, error) {
	file, err := createFile(path)
	if err != nil {
		return nil, err
	}
	db := &DB{
		file:        file,
		pageManager: newPageManager(file),
	}
	return db, nil
}
