package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalProvider_Walk(t *testing.T) {
	dir := t.TempDir()

	file1 := filepath.Join(dir, "a.stat")
	file2 := filepath.Join(dir, "b.stat")
	os.WriteFile(file1, []byte("header1\ndata1"), 0o644)
	os.WriteFile(file2, []byte("header2\ndata2"), 0o644)

	provider := NewLocalProvider([]string{file1, file2})

	var visited []string
	var contents []string

	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		visited = append(visited, name)
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		contents = append(contents, string(data))
		return nil
	})
	if err != nil {
		t.Fatalf("Walk returned error: %v", err)
	}
	if len(visited) != 2 {
		t.Fatalf("expected 2 files, got %d", len(visited))
	}
	if visited[0] != file1 || visited[1] != file2 {
		t.Errorf("unexpected file order: %v", visited)
	}
	if contents[0] != "header1\ndata1" {
		t.Errorf("unexpected content for file1: %q", contents[0])
	}
	if contents[1] != "header2\ndata2" {
		t.Errorf("unexpected content for file2: %q", contents[1])
	}
}

func TestLocalProvider_Walk_SkipsEmptyPaths(t *testing.T) {
	dir := t.TempDir()
	file1 := filepath.Join(dir, "a.stat")
	os.WriteFile(file1, []byte("content"), 0o644)

	provider := NewLocalProvider([]string{"", file1, ""})

	count := 0
	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Walk returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 file visited, got %d", count)
	}
}

func TestLocalProvider_Walk_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	file1 := filepath.Join(dir, "a.stat")
	file2 := filepath.Join(dir, "b.stat")
	os.WriteFile(file1, []byte("data1"), 0o644)
	os.WriteFile(file2, []byte("data2"), 0o644)

	provider := NewLocalProvider([]string{file1, file2})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := provider.Walk(ctx, func(name string, r io.Reader) error {
		t.Fatal("callback should not be called when context is cancelled")
		return nil
	})

	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestLocalProvider_Walk_FileNotFound(t *testing.T) {
	provider := NewLocalProvider([]string{"/nonexistent/path/file.stat"})

	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		t.Fatal("callback should not be called for missing file")
		return nil
	})

	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLocalProvider_Walk_EmptyFileList(t *testing.T) {
	provider := NewLocalProvider([]string{})

	count := 0
	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Walk returned error: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 files visited, got %d", count)
	}
}

func TestLocalProvider_Walk_CallbackError(t *testing.T) {
	dir := t.TempDir()
	file1 := filepath.Join(dir, "a.stat")
	file2 := filepath.Join(dir, "b.stat")
	os.WriteFile(file1, []byte("data1"), 0o644)
	os.WriteFile(file2, []byte("data2"), 0o644)

	provider := NewLocalProvider([]string{file1, file2})

	callbackErr := io.ErrUnexpectedEOF
	count := 0
	err := provider.Walk(context.Background(), func(name string, r io.Reader) error {
		count++
		return callbackErr
	})

	if err != callbackErr {
		t.Fatalf("expected callback error, got: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected walk to stop after first error, visited %d files", count)
	}
}
