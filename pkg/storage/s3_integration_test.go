//go:build integration

package storage_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dtcenter/METjson2db/pkg/storage"
)

const (
	ministackEndpoint = "http://localhost:4566"
	ministackHealth   = ministackEndpoint + "/_ministack/health"
	testBucket        = "met-integration-test"
	testKey           = "uploads/test_stat_files.tar.gz"
	testDataDir       = "../../test_data"
)

func requireMiniStack(t *testing.T) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(ministackHealth)
	if err != nil {
		t.Fatalf(
			"MiniStack is not reachable at %s: %v\n\n"+
				"This test requires a running MiniStack instance.\n"+
				"Install & Start:  docker run --name ministack -d -p 4566:4566 ministackorg/ministack\n"+
				"Stop:     	       docker rm -f ministack",
			ministackEndpoint, err,
		)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(
			"MiniStack health check returned status %d at %s\n\n"+
				"Ensure MiniStack is running: docker start ministack",
			resp.StatusCode, ministackHealth,
		)
	}
}

func newMiniStackS3Client(t *testing.T) *s3.Client {
	t.Helper()
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithBaseEndpoint(ministackEndpoint),
	)
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

func buildTarGzFromDir(t *testing.T, dir string) []byte {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading test_data dir %s: %v", dir, err)
	}

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	statCount := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".stat") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}

		hdr := &tar.Header{
			Name:     entry.Name(),
			Mode:     0o644,
			Size:     int64(len(data)),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("writing tar header for %s: %v", entry.Name(), err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatalf("writing tar content for %s: %v", entry.Name(), err)
		}
		statCount++
	}

	if statCount == 0 {
		t.Fatalf("no .stat files found in %s", dir)
	}

	tw.Close()
	gw.Close()

	t.Logf("built tarball: %d stat files, %d bytes compressed", statCount, buf.Len())
	return buf.Bytes()
}

func setupTestBucket(t *testing.T, client *s3.Client, tarball []byte) {
	t.Helper()
	ctx := context.Background()

	client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(testBucket),
		Key:               aws.String(testKey),
		Body:              bytes.NewReader(tarball),
		ContentType:       aws.String("application/gzip"),
		ChecksumAlgorithm: "SHA256",
	})
	if err != nil {
		t.Fatalf("uploading tarball to s3://%s/%s: %v", testBucket, testKey, err)
	}
}

func TestS3TarballProvider_MiniStack(t *testing.T) {
	requireMiniStack(t)

	client := newMiniStackS3Client(t)
	tarball := buildTarGzFromDir(t, testDataDir)
	setupTestBucket(t, client, tarball)

	provider := storage.NewS3TarballProvider(client, testBucket, testKey)
	ctx := context.Background()

	var files []string
	var totalBytes int

	err := provider.Walk(ctx, func(name string, r io.Reader) error {
		data, readErr := io.ReadAll(r)
		if readErr != nil {
			return readErr
		}
		files = append(files, name)
		totalBytes += len(data)

		lines := strings.Split(string(data), "\n")
		if len(lines) < 2 {
			t.Errorf("stat file %s has fewer than 2 lines", name)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk returned error: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("expected at least one stat file in tarball")
	}

	expectedFiles, err := countStatFiles(testDataDir)
	if err != nil {
		t.Fatalf("counting stat files in %s: %v", testDataDir, err)
	}
	if len(files) != expectedFiles {
		t.Errorf("expected %d stat files, got %d", expectedFiles, len(files))
	}

	t.Logf("streamed %d stat files (%d bytes) from s3://%s/%s", len(files), totalBytes, testBucket, testKey)
	for _, f := range files {
		if !strings.HasSuffix(f, ".stat") {
			t.Errorf("non-stat file in results: %s", f)
		}
		t.Logf("  %s", f)
	}
}

func countStatFiles(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("reading dir: %w", err)
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".stat") {
			count++
		}
	}
	return count, nil
}
