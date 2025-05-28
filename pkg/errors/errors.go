package errors

import "errors"

var (
	ErrNilConnection = errors.New("nil connection")
	ErrNilConnectionPool = errors.New("nil connection pool")
)
