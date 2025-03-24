package black_box_tests

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/NOAA-GSL/METdatacb/pkg/core"
	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

func TestMerge(t *testing.T) {
	slog.Info("TestMerge")

	var inputFiles []string
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_180000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_240000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_360000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.stat")

	err := testMerge_Init()

	if err != nil {
		t.Errorf("testMerge_Init error" + err.Error())
	}
	/*
		err = testMerge_CleanDb()
		if err != nil {
			t.Errorf("testMerge_CleanDb error" + err.Error())
		}

		state.Conf.OverWriteData = true
		err = testMerge_Upload(inputFiles)
		if err != nil {
			t.Errorf("testMerge_UploadNoMerge error" + err.Error())
		}
		// data_lengths 0:0 1:0, 2:224, 3:2940
	*/

	err = testMerge_CleanDb()
	if err != nil {
		t.Errorf("testMerge_CleanDb error" + err.Error())
	}

	state.Conf.OverWriteData = true
	err = testMerge_UploadForMergeTest(inputFiles)
	if err != nil {
		t.Errorf("testMerge_UploadForMergeTest error" + err.Error())
	}
	// data_lengths 0:1582 1:0, 2:118, 3:1464

	state.Conf.OverWriteData = false
	err = testMerge_Upload(inputFiles)
	if err != nil {
		t.Errorf("testMerge_Upload error" + err.Error())
	}
	// data_lengths 0:0 1:0, 2:224, 3:2940
}

func testMerge_Init() error {
	slog.Info("TestMerge_Init()")

	home, _ := os.UserHomeDir()
	state.Conf, _ = core.ParseConfig("../../settings.json")
	state.Credentials = core.GetCredentials(home + "/credentials")
	loadSpec, err := core.ParseLoadSpec("../../load_spec.json")

	if len(loadSpec.TargetCollection) > 0 {
		state.Credentials.Cb_collection = loadSpec.TargetCollection
	}

	return err
}

func testMerge_CleanDb() error {
	slog.Info("TestMerge_CleanDb()")

	sqlStr := fmt.Sprintf("DELETE FROM %s.%s.%s WHERE MODEL = \"MERGE_TEST\"",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	conn := utils.GetDbConnection(state.Credentials)
	_, err := conn.Scope.Query(sqlStr, nil)

	return err
}

func testMerge_UploadForMergeTest(inputFiles []string) error {
	slog.Info("testMerge_UploadForMergeTest()")
	err := core.ProcessInputFiles(inputFiles, postProcessDocsForMergeTest)
	return err
}

func testMerge_Upload(inputFiles []string) error {
	slog.Info("TestMerge_Upload()")
	err := core.ProcessInputFiles(inputFiles, postProcessDocsForMergeTest_markDocs)
	return err
}

func postProcessDocsForMergeTest_markDocs() {
	for _, docTmp := range state.CbDocs {
		doc := docTmp.(map[string]interface{})
		doc["MODEL"] = "MERGE_TEST"
	}
}

func postProcessDocsForMergeTest() {
	count := 0
	for _, docTmp := range state.CbDocs {
		doc := docTmp.(map[string]interface{})
		doc["MODEL"] = "MERGE_TEST"
		count = count + 1
		if count%2 == 0 {
			delete(doc, "data")
			doc["data"] = make(map[string]interface{})
		}
	}
}

func createTestDataFile(infile string, outfile string) error {
	in, err := os.Open(infile)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(outfile)
	if err != nil {
		return err
	}
	defer out.Close()

	rawData, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	lines := strings.Split(string(rawData), "\n")

	for line := range lines {
		if line == 0 || lines[line] == "" {
			out.WriteString(lines[line] + "\n")
		} else {
			lineStr := strings.Replace(lines[line], "GFS", "MERGE_TEST", 1)
			out.WriteString(lineStr + "\n")
		}
	}
	return nil
}
