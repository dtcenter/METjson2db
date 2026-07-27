package storage

import (
	"context"
	"fmt"
	"io"
	"os"
)

// LocalProvider reads stat files from the local filesystem.
type LocalProvider struct {
	Files []string
}

func NewLocalProvider(files []string) *LocalProvider {
	return &LocalProvider{Files: files}
}

func (p *LocalProvider) Walk(ctx context.Context, fn func(name string, r io.Reader) error) error {
	for _, path := range p.Files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if path == "" {
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening %s: %w", path, err)
		}

		// Close the file after each iteration so we don't exhaust file descriptors
		// Otherwise, defer fires only after Walk exits.
		if err := func() error {
			defer f.Close()
			return fn(path, f)
		}(); err != nil {
			return err
		}
	}
	return nil
}
