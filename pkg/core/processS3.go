package core

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

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
