package metadataUpdate

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/NOAA-GSL/METjson2db/pkg/types"
	"github.com/NOAA-GSL/METjson2db/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	log.Println("templateQueries:init()")
}

func GetDatasets(conn types.CbConnection, app string, subtype string, lineType string) (jsonOut []string) {
	log.Printf("GetDatasets(" + app + "," + subtype + "," + lineType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqlTemplates/getDatasets.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDBTARGET}}", conn.VxDBTARGET)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxSUBTYPE}}", subtype)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxLINETYPE}}", lineType)

	fmt.Println("SQL:\n" + tmplSQL)

	datasets := utils.QueryWithSQLStringSA(conn.Scope, tmplSQL)

	log.Printf("\tin %v", time.Since(start))
	return datasets
}

func GetModels(conn types.CbConnection, app string, subtype string, version string, dataset string) (jsonOut []string) {
	log.Println("getModels(" + app + "," + subtype + "," + version + "," + dataset + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqlTemplates/getModels.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDBTARGET}}", conn.VxDBTARGET)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxSUBTYPE}}", subtype)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxVERSION}}", version)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDATASET}}", dataset)

	fmt.Println("SQL:\n" + tmplSQL)
	rv := utils.QueryWithSQLStringSA(conn.Scope, tmplSQL)

	log.Printf("\tin %v", time.Since(start))
	return rv
}

func GetLineTypes(conn types.CbConnection, app string, subtype string, version string, dataset string, model string) (jsonOut []string) {
	log.Println("GetLineTypes(" + app + "," + subtype + "," + version + "," + dataset + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqlTemplates/getLineTypes.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDBTARGET}}", conn.VxDBTARGET)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxSUBTYPE}}", subtype)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxVERSION}}", version)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDATASET}}", dataset)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxMODEL}}", model)

	fmt.Println("SQL:\n" + tmplSQL)
	rv := utils.QueryWithSQLStringSA(conn.Scope, tmplSQL)

	log.Printf("\tin %v", time.Since(start))
	return rv
}

func GetBasins(conn types.CbConnection, app string, subtype string, version string, dataset string, model string, lineType string) (jsonOut []string) {
	log.Println("GetBasins(" + app + "," + subtype + "," + version + "," + dataset + "," + lineType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqlTemplates/getBasins.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDBTARGET}}", conn.VxDBTARGET)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxSUBTYPE}}", subtype)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxVERSION}}", version)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDATASET}}", dataset)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxMODEL}}", model)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxLINE_TYPE}}", lineType)

	fmt.Println("SQL:\n" + tmplSQL)
	rv := utils.QueryWithSQLStringSA(conn.Scope, tmplSQL)

	log.Printf("\tin %v", time.Since(start))
	return rv
}

func GetStormIDs(conn types.CbConnection, app string, subtype string, version string, dataset string, model string, lineType string, basin string) (jsonOut []string) {
	log.Println("GetStormIDs(" + app + "," + subtype + "," + version + "," + dataset + "," + lineType + "," + basin + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqlTemplates/getStormIDs.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDBTARGET}}", conn.VxDBTARGET)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxSUBTYPE}}", subtype)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxVERSION}}", version)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDATASET}}", dataset)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxMODEL}}", model)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxLINE_TYPE}}", lineType)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxBASIN}}", basin)

	fmt.Println("SQL:\n" + tmplSQL)
	rv := utils.QueryWithSQLStringSA(conn.Scope, tmplSQL)

	log.Printf("\tin %v", time.Since(start))
	return rv
}

func GetDocsForStormIds(conn types.CbConnection, app string, subtype string, version string, dataset string, model string,
	lineType string, basin string, stormids []string,
) (jsonOut []interface{}) {
	log.Println("GetStormIDs(" + app + "," + subtype + "," + version + "," + dataset + "," + lineType + "," + basin + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqlTemplates/getDocsForStormIds.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDBTARGET}}", conn.VxDBTARGET)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxSUBTYPE}}", subtype)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxVERSION}}", version)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxDATASET}}", dataset)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxMODEL}}", model)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxLINE_TYPE}}", lineType)
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxBASIN}}", basin)
	stormidsStr := "["
	for i := 0; i < len(stormids); i++ {
		stormidsStr = stormidsStr + "\"" + stormids[i] + "\""
		if i < (len(stormids) - 1) {
			stormidsStr = stormidsStr + ","
		} else {
			stormidsStr = stormidsStr + "]"
		}
	}
	tmplSQL = strings.ReplaceAll(tmplSQL, "{{vxSTORM_IDS_LIST}}", stormidsStr)

	fmt.Println("SQL:\n" + tmplSQL)
	rv := utils.QueryWithSQLStringMAP(conn.Scope, tmplSQL)

	log.Printf("\tin %v", time.Since(start))
	return rv
}
