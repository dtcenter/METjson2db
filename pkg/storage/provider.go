package storage

import (
	"context"
	"io"
)

// StorageProvider abstracts access to stat files from different storage backends.
type StorageProvider interface {
	Walk(ctx context.Context, fn func(name string, r io.Reader) error) error
}
