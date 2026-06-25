package storage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

func createTestTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	for name, content := range files {
		hdr := &tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("writing tar header for %s: %v", name, err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("writing tar content for %s: %v", name, err)
		}
	}

	tw.Close()
	gw.Close()
	return buf.Bytes()
}

func TestIsStatFile(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"grid_stat_output.stat", true},
		{"path/to/deep/file.stat", true},
		{"file.txt", false},
		{"file.stat.bak", false},
		{"readme.md", false},
		{".stat", true},
	}

	for _, tt := range tests {
		if got := isStatFile(tt.name); got != tt.want {
			t.Errorf("isStatFile(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestS3TarballProvider_Walk_ExtractsStatFiles(t *testing.T) {
	tarData := createTestTarGz(t, map[string]string{
		"data/file1.stat": "header1\ndata1",
		"data/file2.stat": "header2\ndata2",
		"data/readme.txt": "not a stat file",
		"data/file3.stat": "header3\ndata3",
	})

	client := newFakeS3Client(tarData)
	provider := &S3TarballProvider{
		Client: client,
		Bucket: "test-bucket",
		Key:    "archive.tar.gz",
	}

	var visited []string
	var contents []string

	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		visited = append(visited, name)
		data, readErr := io.ReadAll(r)
		if readErr != nil {
			return readErr
		}
		contents = append(contents, string(data))
		return nil
	})
	if err != nil {
		t.Fatalf("Walk returned error: %v", err)
	}
	if len(visited) != 3 {
		t.Fatalf("expected 3 stat files, got %d: %v", len(visited), visited)
	}

	wantNames := map[string]bool{
		"data/file1.stat": true,
		"data/file2.stat": true,
		"data/file3.stat": true,
	}
	for _, name := range visited {
		if !wantNames[name] {
			t.Errorf("unexpected file visited: %s", name)
		}
	}
}

func TestS3TarballProvider_Walk_ContextCancellation(t *testing.T) {
	tarData := createTestTarGz(t, map[string]string{
		"data/file1.stat": "header1\ndata1",
		"data/file2.stat": "header2\ndata2",
	})

	client := newFakeS3Client(tarData)
	provider := &S3TarballProvider{
		Client: client,
		Bucket: "test-bucket",
		Key:    "archive.tar.gz",
	}

	ctx, cancel := context.WithCancel(context.Background())

	count := 0
	err := provider.Walk(ctx, func(name string, r io.Reader) error {
		count++
		cancel()
		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected nil or context.Canceled, got: %v", err)
	}
	if count > 1 {
		t.Fatalf("expected at most 1 file processed after cancel, got %d", count)
	}
}

func TestS3TarballProvider_Walk_CallbackError(t *testing.T) {
	tarData := createTestTarGz(t, map[string]string{
		"file1.stat": "header1\ndata1",
		"file2.stat": "header2\ndata2",
	})

	client := newFakeS3Client(tarData)
	provider := &S3TarballProvider{
		Client: client,
		Bucket: "test-bucket",
		Key:    "archive.tar.gz",
	}

	callbackErr := io.ErrUnexpectedEOF
	count := 0
	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		count++
		return callbackErr
	})

	if !errors.Is(err, callbackErr) {
		t.Fatalf("expected callback error, got: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected walk to stop after first error, got %d calls", count)
	}
}

func TestS3TarballProvider_Walk_EmptyTarball(t *testing.T) {
	tarData := createTestTarGz(t, map[string]string{})

	client := newFakeS3Client(tarData)
	provider := &S3TarballProvider{
		Client: client,
		Bucket: "test-bucket",
		Key:    "archive.tar.gz",
	}

	count := 0
	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Walk returned error: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 files, got %d", count)
	}
}

func TestS3TarballProvider_Walk_NoStatFiles(t *testing.T) {
	tarData := createTestTarGz(t, map[string]string{
		"readme.txt": "hello",
		"data.csv":   "col1,col2",
	})

	client := newFakeS3Client(tarData)
	provider := &S3TarballProvider{
		Client: client,
		Bucket: "test-bucket",
		Key:    "archive.tar.gz",
	}

	count := 0
	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Walk returned error: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 stat files, got %d", count)
	}
}

func TestS3TarballProvider_Walk_GetObjectError(t *testing.T) {
	client := newFakeS3ClientWithError(fmt.Errorf("access denied"))
	provider := &S3TarballProvider{
		Client: client,
		Bucket: "test-bucket",
		Key:    "archive.tar.gz",
	}

	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		t.Fatal("callback should not be called on GetObject error")
		return nil
	})

	if err == nil {
		t.Fatal("expected error from GetObject failure")
	}
}

func TestS3TarballProvider_Walk_InvalidGzip(t *testing.T) {
	client := newFakeS3Client([]byte("not gzip data"))
	provider := &S3TarballProvider{
		Client: client,
		Bucket: "test-bucket",
		Key:    "archive.tar.gz",
	}

	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		t.Fatal("callback should not be called for invalid gzip")
		return nil
	})

	if err == nil {
		t.Fatal("expected error for invalid gzip")
	}
}
