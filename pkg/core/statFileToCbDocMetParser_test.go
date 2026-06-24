package core

import (
	"os"
	"testing"

	"github.com/dtcenter/METjson2db/pkg/state"
)

func TestParseStatFileContent_Scanner(t *testing.T) {
	// Initialize global maps and channels (⊙＿⊙')
	state.StateReset()

	// Satisfy the parser's dataset name requirement
	state.LoadSpec.DatasetName = "test_data"

	// Bypass async channel logic for unit testing
	state.LoadSpec.OverWriteData = true

	// Use an actual stat file from the existing test_data directory
	filePath := "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat"

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	// Execute the newly refactored streaming parser
	doc, err := parseStatFileContent(filePath, file)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if doc == nil {
		t.Fatal("Expected parsed document, got nil")
	}

	// Optional: Add specific assertions here to ensure data integrity
	// e.g., check that doc["data"] exists and has the expected length
}

func BenchmarkParseStatFileContent(b *testing.B) {
	filePath := "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat"
	file, err := os.Open(filePath)
	if err != nil {
		b.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	b.ResetTimer()
	b.ReportAllocs() // This is the magic flag!

	for i := 0; i < b.N; i++ {
		// Reset the file pointer to the beginning for each iteration
		file.Seek(0, 0)
		_, _ = parseStatFileContent(filePath, file)
	}
}
