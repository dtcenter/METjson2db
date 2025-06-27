//go:build integration
// +build integration

package black_box_tests

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/dtcenter/METjson2db/pkg/core"
	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/utils"
)

func TestMerge(t *testing.T) {
	slog.Info("TestMerge")

	var inputFiles []string
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_180000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_240000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_360000L_20240203_120000V.stat")
	inputFiles = append(inputFiles, "../../test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.stat")

	testMerge_Init()

	/*
		err := testMerge_CleanDb()
		if err != nil {
			t.Errorf("testMerge_CleanDb error" + err.Error())
		}

		state.Conf.OverWriteData = true
		err = testMerge_Upload(inputFiles)
		if err != nil {
			t.Errorf("testMerge_UploadNoMerge error" + err.Error())
		}
	*/

	err := testMerge_CleanDb()
	if err != nil {
		t.Errorf("testMerge_CleanDb error" + err.Error())
	}

	state.Conf.OverWriteData = true
	err = testMerge_UploadForMergeTest(inputFiles)
	if err != nil {
		t.Errorf("testMerge_UploadForMergeTest error" + err.Error())
	}
	dataLengthsPre := getDataLengths()

	state.Conf.OverWriteData = false
	err = testMerge_Upload(inputFiles)
	if err != nil {
		t.Errorf("testMerge_Upload error" + err.Error())
	}
	dataLengthsPost := getDataLengths()
	slog.Info(fmt.Sprintf("Merge test, dataLengthsPre:%v, dataLengthsPost:%v", dataLengthsPre, dataLengthsPost))
	if dataLengthsPost[3] <= dataLengthsPre[3] {
		t.Errorf("testMerge data lengths not as expected!")
	}
}

func testMerge_Init() {
	slog.Info("TestMerge_Init()")

	home, _ := os.UserHomeDir()
	state.Credentials = core.GetCredentials(home + "/credentials")

	state.Credentials.Cb_collection = "MET_tests"
}

func testMerge_CleanDb() error {
	slog.Info("TestMerge_CleanDb()")

	sqlStr := fmt.Sprintf("DELETE FROM %s.%s.%s",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	conn := utils.GetDbConnection(state.Credentials)
	_, err := conn.Scope.Query(sqlStr, nil)

	sqlStr = fmt.Sprintf("SELECT COUNT(*) as count FROM %s.%s.%s",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	rv := utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
	slog.Info(fmt.Sprintf("count after db clean:%d:", rv[0].(map[string]interface{})["count"]))
	return err
}

func testMerge_UploadForMergeTest(inputFiles []string) error {
	slog.Info("testMerge_UploadForMergeTest()")
	err := core.ProcessInputFiles(inputFiles, nil)
	if err != nil {
		return err
	}

	state.MergeTestDocs = make(map[string]interface{})

	conn := utils.GetDbConnection(state.Credentials)
	sqlStr := fmt.Sprintf("SELECT c.id as id FROM %s.%s.%s AS c WHERE ARRAY_LENGTH(object_pairs(c.data)) = 3",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	slog.Debug(sqlStr)
	rv := utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
	// slog.Info("id for data count=3:\n" + utils.JsonPrettyPrint(rv))
	for i := 0; i < len(rv); i++ {
		id := rv[i].(map[string]interface{})["id"].(string)
		sqlStr = fmt.Sprintf("SELECT * FROM %s.%s.%s AS c USE KEYS \"%s\"",
			state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection, id)
		// slog.Info(sqlStr)
		rv := utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
		if rv == nil || len(rv) == 0 {
			continue
		}
		docPre := rv[0].(map[string]interface{})["c"].(map[string]interface{})
		docPost := make(map[string]interface{})
		inrec, _ := json.Marshal(docPre)
		json.Unmarshal(inrec, &docPost)
		data := docPost["data"].(map[string]interface{})
		ikey := 0
		for k := range data {
			ikey = ikey + 1
			if ikey == 2 {
				delete(data, k)
			}
		}
		_, err := conn.Collection.Upsert(id, docPost, nil)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
			slog.Error(fmt.Sprintf("******* Upsert error:ID:%s", id))
		}
		state.MergeTestDocs[id] = docPost
	}
	slog.Info(fmt.Sprintf("MergeTestDocs:%d", len(state.MergeTestDocs)))
	return err
}

func preDbLoadCallback() {
	count := 0
	touchCount := 0
	for id, doci := range state.CbDocs {
		if (count % 2) == 0 {
			dbDoci := state.MergeTestDocs[id]
			if dbDoci != nil {
				doc := doci.(map[string]interface{})
				delete(doc, "data")
				doc["data"] = make(map[string]interface{})
				touchCount = touchCount + 1
			}
		}
		count = count + 1
	}
	slog.Info(fmt.Sprintf("preDbLoadCallback(), count:%d, touchCount:%d", count, touchCount))
}

func testMerge_Upload(inputFiles []string) error {
	slog.Info("TestMerge_Upload()")
	err := core.ProcessInputFiles(inputFiles, preDbLoadCallback)
	return err
}

func getDataLengths() []float64 {
	conn := utils.GetDbConnection(state.Credentials)
	sqlStr := fmt.Sprintf("SELECT count(*) as count FROM %s.%s.%s AS c WHERE ARRAY_LENGTH(object_pairs(c.data)) = 0",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	slog.Debug(sqlStr)
	rv := utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
	count_0 := rv[0].(map[string]interface{})["count"]
	sqlStr = fmt.Sprintf("SELECT count(*) as count FROM %s.%s.%s AS c WHERE ARRAY_LENGTH(object_pairs(c.data)) = 1",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	slog.Debug(sqlStr)
	rv = utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
	count_1 := rv[0].(map[string]interface{})["count"]
	sqlStr = fmt.Sprintf("SELECT count(*) as count FROM %s.%s.%s AS c WHERE ARRAY_LENGTH(object_pairs(c.data)) = 2",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	slog.Debug(sqlStr)
	rv = utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
	count_2 := rv[0].(map[string]interface{})["count"]
	sqlStr = fmt.Sprintf("SELECT count(*) as count FROM %s.%s.%s AS c WHERE ARRAY_LENGTH(object_pairs(c.data)) = 3",
		state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)
	slog.Debug(sqlStr)
	rv = utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
	count_3 := rv[0].(map[string]interface{})["count"]
	slog.Info(fmt.Sprintf("data counts:%v,%v,%v,%v", count_0, count_1, count_2, count_3))
	return []float64{count_0.(float64), count_1.(float64), count_2.(float64), count_3.(float64)}
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
