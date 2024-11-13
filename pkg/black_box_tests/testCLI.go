package black_box_tests

import (
	"testing"
	// "github.com/NOAA-GSL/METdatacb/cmd/metdatacb"
)

func TestCLI(t *testing.T) {
	got := 1
	if got != 1 {
		t.Errorf("Abs(-1) = %d; want 1", got)
	}
}
