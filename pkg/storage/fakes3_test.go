package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeS3Client struct {
	data []byte
	err  error
}

func newFakeS3Client(data []byte) S3ObjectGetter {
	return &fakeS3Client{data: data}
}

func newFakeS3ClientWithError(err error) S3ObjectGetter {
	return &fakeS3Client{err: err}
}

func (f *fakeS3Client) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(f.data)),
	}, nil
}
