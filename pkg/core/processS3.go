package core

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/dtcenter/METjson2db/pkg/state"
)

// ManifestEntry represents a single entry in your manifest.json
type ManifestEntry struct {
	Name    string      `json:"name"`
	Path    string      `json:"path"`
	Size    int64       `json:"size"`
	Mode    os.FileMode `json:"mode"`
	ModTime time.Time   `json:"modTime"`
	IsDir   bool        `json:"isDir"`
}

// Manifest represents the overall structure of your manifest.json
type Manifest struct {
	Timestamp time.Time       `json:"timestamp"`
	Files     []ManifestEntry `json:"files"`
}

// init runs before main() is evaluated
func init() {
	slog.Debug("ProcessInput:init()")
}

func ProcessS3Files(inputS3Path string, outputS3Path string, preDbLoadCallback func()) error {
	slog.Info(fmt.Sprintf("ProcessInputFiles(%s,%s)", inputS3Path, outputS3Path))

	// start := time.Now()
	state.StateReset()

	_, s3client, err := iniS3Client()
	if err != nil {
		log.Fatalf("Unable to init s3 %v", err)
	}
	bucketIn, keyIn, _ := extractS3InfoFromS3Path(inputS3Path)
	bucketOut, keyOut, _ := extractS3InfoFromS3Path(outputS3Path)
	slog.Info(fmt.Sprintf("bucketIn=%s,keyIn=%s bucketOut=%s,keyOut=%s", bucketIn, keyIn, bucketOut, keyOut))

	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := s3client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketIn),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("first page results")
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), *object.Size)
	}

	result, err := s3client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketIn),
		Key:    aws.String(keyIn),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			// handle NoSuchKey error
			return nil
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			code := apiErr.ErrorCode()
			message := apiErr.ErrorMessage()
			log.Printf("key=%s message=%d", code, message)
			return nil
		}
		// handle error
		return nil
	}
	defer result.Body.Close()

	contentType := *result.ContentType
	contentLength := *result.ContentLength
	eTag := *result.ETag

	log.Printf("contentType=%s,contentLength=%d,eTag:%s", contentType, contentLength, eTag)

	body, err := io.ReadAll(result.Body)
	_, err = io.ReadAll(result.Body)
	if err != nil {
		log.Fatalf("failed to read: %v", err)
	}

	// Convert the byte slice to a string for printing
	// fmt.Println(string(body))

	/*
		// Simulate a tar archive in a byte slice
		// In a real scenario, this byte slice would come from a network, memory, etc.
		tarData := createTarInBytes()

		// Write the JSON data to a file
		err = os.WriteFile("testTar0.tar", tarData, 0644)
		if err != nil {
			log.Fatalf("Error writing testTar0.tar: %v\n", err)
		}

		// Convert the byte slice to an io.Reader
		tarReaderFromBytes := bytes.NewReader(tarData)
	*/

	// Convert the byte slice to an io.Reader
	tarReaderFromBytes := bytes.NewReader(body)

	// Create a tar.Reader
	tarReader := tar.NewReader(tarReaderFromBytes)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			log.Fatalf("Error reading tar header: %v", err)
		}

		log.Printf("header: %s", header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			log.Println("TypeDir:%v", header.Mode)
		case tar.TypeReg:
			log.Println("TypeReg:%v", header.Mode)
		default:
			fmt.Printf("Skipping unknown tar entry type: %s, %v\n", header.Name, header.Typeflag)
		}
	}

	// createTarManifestTest()
	return nil
}

func iniS3Client() (aws.Config, *s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint("http://localhost:4566"),
	)
	if err != nil {
		log.Fatalf("Unable to init s3: %v", err)
	}

	s3client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	return cfg, s3client, nil
}

func extractS3InfoFromS3Path(s3URI string) (string, string, error) {
	u, err := url.Parse(s3URI)
	if err != nil {
		fmt.Printf("Error parsing S3 URI: %v\n", err)
		return "", "", err
	}

	// Extract the bucket name
	bucketName := u.Host

	// Extract the object key (path)
	// The u.Path will include a leading slash, which is typically removed for S3 keys.
	objectKey := strings.TrimPrefix(u.Path, "/")

	return bucketName, objectKey, nil
}

func createTarManifestTest() error {
	tarFilePath := "example.tar.gz" // Replace with your tar file path
	manifestOutputPath := "manifest.json"

	// Create a dummy tar.gz file for demonstration
	createDummyTarGz(tarFilePath)

	manifest, err := createManifestFromTar(tarFilePath)
	if err != nil {
		fmt.Printf("Error creating manifest: %v\n", err)
		return err
	}

	// Marshal the manifest to JSON
	jsonData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling manifest to JSON: %v\n", err)
		return err
	}

	// Write the JSON data to a file
	err = os.WriteFile(manifestOutputPath, jsonData, 0644)
	if err != nil {
		fmt.Printf("Error writing manifest.json: %v\n", err)
		return err
	}

	fmt.Printf("Manifest created successfully at %s\n", manifestOutputPath)
	return nil
}

func createManifestFromTar(tarFilePath string) (*Manifest, error) {
	file, err := os.Open(tarFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open tar file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file

	// Check if it's a gzipped tar file
	if filepath.Ext(tarFilePath) == ".gz" {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	tarReader := tar.NewReader(reader)

	manifest := &Manifest{
		Timestamp: time.Now(),
		Files:     []ManifestEntry{},
	}

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar entry: %w", err)
		}

		entry := ManifestEntry{
			Name:    filepath.Base(header.Name),
			Path:    header.Name,
			Size:    header.Size,
			Mode:    os.FileMode(header.Mode),
			ModTime: header.ModTime,
			IsDir:   header.Typeflag == tar.TypeDir,
		}
		manifest.Files = append(manifest.Files, entry)
	}

	return manifest, nil
}

// createDummyTarGz creates a sample tar.gz file for testing
func createDummyTarGz(filename string) {
	file, _ := os.Create(filename)
	defer file.Close()

	gw := gzip.NewWriter(file)
	defer gw.Close()

	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Add a directory
	tw.WriteHeader(&tar.Header{
		Name:     "my_dir/",
		Mode:     0755,
		ModTime:  time.Now(),
		Typeflag: tar.TypeDir,
	})

	// Add a file
	header := &tar.Header{
		Name:    "my_dir/file1.txt",
		Mode:    0644,
		Size:    int64(len("hello world")),
		ModTime: time.Now(),
	}
	tw.WriteHeader(header)
	tw.Write([]byte("hello world"))
}

// createTarInBytes creates a simple tar archive in a byte slice for demonstration
func createTarInBytes() []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Add a file
	header := &tar.Header{
		Name: "file1.txt",
		Mode: 0644,
		Size: int64(len("Hello from file1!")),
	}
	if err := tw.WriteHeader(header); err != nil {
		log.Fatalf("Failed to write header for file1.txt: %v", err)
	}
	if _, err := tw.Write([]byte("Hello from file1!")); err != nil {
		log.Fatalf("Failed to write content for file1.txt: %v", err)
	}

	// Add a directory
	dirHeader := &tar.Header{
		Name:     "my_directory/",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	}
	if err := tw.WriteHeader(dirHeader); err != nil {
		log.Fatalf("Failed to write header for my_directory: %v", err)
	}

	// Add another file inside the directory
	file2Header := &tar.Header{
		Name: "my_directory/file2.txt",
		Mode: 0644,
		Size: int64(len("Content of file2.")),
	}
	if err := tw.WriteHeader(file2Header); err != nil {
		log.Fatalf("Failed to write header for file2.txt: %v", err)
	}
	if _, err := tw.Write([]byte("Content of file2.")); err != nil {
		log.Fatalf("Failed to write content for file2.txt: %v", err)
	}

	if err := tw.Close(); err != nil {
		log.Fatalf("Failed to close tar writer: %v", err)
	}

	return buf.Bytes()
}
