package black_box_tests

import (
	"log/slog"
	"testing"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
)

func TestDataLoad(t *testing.T) {
	slog.Info("TestDataLoad")

	var inputFiles []string
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_180000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_240000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_360000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.stat")

	testMerge_Init()

	err := testMerge_CleanDb()
	if err != nil {
		t.Errorf("testMerge_CleanDb error" + err.Error())
	}

	state.Conf.OverWriteData = true
	err = testMerge_Upload(inputFiles)
	if err != nil {
		t.Errorf("testMerge_UploadNoMerge error" + err.Error())
	}
	dataLengths := getDataLengths()
	if dataLengths[3] == 0 {
		t.Errorf("TestDataLoad, data lengths not as expected")
	}
}
