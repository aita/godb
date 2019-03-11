package db

import "errors"

var (
	ErrInvalidMagic = errors.New("db: invalid magic number")
	ErrNoEmptySpace = errors.New("db: no empty space")
)
