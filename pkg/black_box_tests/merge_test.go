package black_box_tests

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/NOAA-GSL/METdatacb/pkg/core"
	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

func TestMerge(t *testing.T) {
	// path, _ := os.Getwd()
	home, _ := os.UserHomeDir()

	var inputFiles []string
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_180000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_240000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_360000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.stat")

	/*
		infile := "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat"
		outfile := "../../test_data/MERGE_TEST_grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat"
		err := createTestDataFile(infile, outfile)
		if err != nil {
			t.Errorf("TestMerge error" + err.Error())
		}
		inputFiles = append(inputFiles, infile)
	*/

	state.Conf, _ = core.ParseConfig("../../settings.json")
	state.Credentials = core.GetCredentials(home + "/credentials")
	loadSpec, _ := core.ParseLoadSpec("../../load_spec.json")

	if len(loadSpec.TargetCollection) > 0 {
		state.Credentials.Cb_collection = loadSpec.TargetCollection
	}

	// TODO: delete merge test docs in DB
	sqlStr := fmt.Sprintf("DELETE FROM %s.%s.%s WHERE MODEL = \"MERGE_TEST\"",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	conn := utils.GetDbConnection(state.Credentials)
	conn.Scope.Query(sqlStr, nil)

	// Upload merge test docs to db
	// state.Conf.RunMode, state.Conf.RunNonThreaded, state.Conf.OverWriteData
	err := core.ProcessInputFiles(inputFiles, postProcessDocsForMergeTest_markAsTest)

	isErr := (err != nil)
	if isErr {
		t.Errorf("TestMerge error" + err.Error())
	}
}

func postProcessDocsForMergeTest_markAsTest() {
	for _, docTmp := range state.CbDocs {
		doc := docTmp.(map[string]interface{})
		doc["MODEL"] = "MERGE_TEST"
	}
}

func postProcessDocsForMergeTest_deleteEvenDataSections() {
	count := 0
	mergeProcessCount := 0
	for _, docTmp := range state.CbDocs {
		doc := docTmp.(map[string]interface{})
		count = count + 1
		if count%2 == 0 {
			delete(doc, "data")
			doc["data"] = make(map[string]interface{})
			mergeProcessCount = mergeProcessCount + 1
		}
	}
	// slog.Info(fmt.Sprintf("mergeProcessCount:%d", mergeProcessCount))
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
