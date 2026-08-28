package storage

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dtcenter/METjson2db/pkg/telemetry"
)

// S3ObjectGetter abstracts the S3 GetObject call for testability.
type S3ObjectGetter interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3TarballProvider streams a tarball from S3 and yields .stat/.tcst/.txt stat file entries.
type S3TarballProvider struct {
	Client S3ObjectGetter
	Bucket string
	Key    string
}

func NewS3TarballProvider(client S3ObjectGetter, bucket, key string) *S3TarballProvider {
	return &S3TarballProvider{
		Client: client,
		Bucket: bucket,
		Key:    key,
	}
}

func (p *S3TarballProvider) Walk(ctx context.Context, fn func(name string, r io.Reader) error) error {
	slog.InfoContext(ctx, "S3TarballProvider.Walk", "bucket", p.Bucket, "key", p.Key)

	// GetObject returns once the response stream is available, not once it's fully read — the
	// tarball's bytes are actually pulled off the wire below, interleaved with gzip/tar decoding,
	// as fn consumes each entry. Recording on return (via defer) covers the whole streamed
	// download instead of just time-to-first-byte.
	s3Start := time.Now()
	defer func() {
		telemetry.S3DownloadDuration.Record(ctx, time.Since(s3Start).Seconds())
	}()

	result, err := p.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.Key),
	})
	if err != nil {
		return fmt.Errorf("s3 GetObject s3://%s/%s: %w", p.Bucket, p.Key, err)
	}
	defer result.Body.Close()

	gz, err := gzip.NewReader(result.Body)
	if err != nil {
		telemetry.TarballExtractionErrors.Add(ctx, 1)
		return fmt.Errorf("gzip reader for s3://%s/%s: %w", p.Bucket, p.Key, err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	fileCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			telemetry.TarballExtractionErrors.Add(ctx, 1)
			return fmt.Errorf("reading tar entry from s3://%s/%s: %w", p.Bucket, p.Key, err)
		}

		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		if !isStatFile(hdr.Name) {
			slog.Debug("skipping non-stat tar entry", "name", hdr.Name)
			continue
		}

		fileCount++
		if err := fn(hdr.Name, tr); err != nil {
			return err
		}
	}

	slog.InfoContext(ctx, "S3TarballProvider.Walk complete", "bucket", p.Bucket, "key", p.Key, "statFiles", fileCount)
	return nil
}

// Check if the file ends in a valid stat file ending
//
// Valid file endings:
// - .stat
// - .tcst
// - .txt
func isStatFile(name string) bool {
	base := filepath.Base(name)
	return strings.HasSuffix(base, ".stat") || strings.HasSuffix(base, ".tcst") || strings.HasSuffix(base, ".txt")
}
