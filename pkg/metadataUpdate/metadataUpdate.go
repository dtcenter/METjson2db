package metadataUpdate

import (
	"fmt"
	"log"
	"time"

	"github.com/NOAA-GSL/METjson2db/pkg/state"
	"github.com/NOAA-GSL/METjson2db/pkg/types"
	"github.com/NOAA-GSL/METjson2db/pkg/utils"
	// "github.com/couchbase/gocb/v2"
)

type StrArray []string

// init runs before main() is evaluated
func MetadataUpdate() {
	start := time.Now()
	log.Print("metadataUpdate:main()")

	conn := utils.GetDbConnection(state.Credentials)

	for ds := 0; ds < len(state.Conf.Metadata); ds++ {
		updateMedataForAppDocType(conn, state.Conf.Metadata[ds].Name, state.Conf.Metadata[ds].App, state.Conf.Metadata[ds].SubType, state.Conf.Metadata[ds].Version)
	}
	log.Println(fmt.Sprintf("\tmeta update finished in %v", time.Since(start)))
}

func updateMedataForAppDocType(conn types.CbConnection, name string, app string, subType string, version string) {
	log.Println("updateMedataForAppDocType(" + name + "," + subType + "," + version + ")")

	// get all datasets in db
	datasets := GetDatasets(conn, app, subType, version)
	log.Println("datasets:")
	utils.PrintStringArray(datasets)

	metadata := types.Metadata{ID: "MD:matsGui:METexpressGui:" + version, Type: "MD", App: "METexpressGui", Version: version}

	for dsi := 0; dsi < len(datasets); dsi++ {
		models := GetModels(conn, app, subType, version, datasets[dsi])
		log.Println("dataset:" + datasets[dsi] + ",models:")
		utils.PrintStringArray(models)
		dataset := types.Dataset{Dataset: datasets[dsi]}

		for mi := 0; mi < len(models); mi++ {
			lineTypes := GetLineTypes(conn, app, subType, version, datasets[dsi], models[mi])
			log.Println("dataset:" + datasets[dsi] + ",model:" + models[mi] + ",lineTypes:")
			utils.PrintStringArray(lineTypes)
			model := types.Model{Model: models[mi]}

			for lti := 0; lti < len(lineTypes); lti++ {
				basins := GetBasins(conn, app, subType, version, datasets[dsi], models[mi], lineTypes[lti])
				log.Println("dataset:" + datasets[dsi] + ",model:" + models[mi] + ",lineType:" + lineTypes[lti] + ",basins:")
				utils.PrintStringArray(basins)
				linetype := types.LineType{LineType: lineTypes[lti]}

				for bi := 0; bi < len(basins); bi++ {
					stormIds := GetStormIDs(conn, app, subType, version, datasets[dsi], models[mi], lineTypes[lti], basins[bi])
					log.Println("dataset:" + datasets[dsi] + ",model:" + models[mi] + ",lineType:" + lineTypes[lti] + ",basin:" + basins[bi] + ",stormIds:")
					utils.PrintStringArray(stormIds)
					basin := types.Basin{Basin: basins[bi]}

					for sti := 0; sti < len(stormIds); sti++ {
						s := stormIds[sti]
						year := s[len(s)-4:]
						stormid := types.StormId{StormId: year}
						basin.StormIds = append(basin.StormIds, stormid)
					}
					linetype.Basins = append(linetype.Basins, basin)
				}
				model.LineTypes = append(model.LineTypes, linetype)
			}
			dataset.Models = append(dataset.Models, model)
		}
		metadata.Datasets = append(metadata.Datasets, dataset)
	}
	fmt.Println(utils.JsonPrettyPrintStruct(metadata))

	/*
		// get needed models
		models := getModels(conn, name, app, doctype, subDocType)
		log.Println("models:")
		printStringArray(models)

		// get models having metadata but no data (remove metadata for these)
		// (note 'like %' is changed to 'like %25')
		models_with_metatada_but_no_data := getModelsNoData(conn, name, app, doctype, subDocType)
		log.Println("models_with_metatada_but_no_data:")
		printStringArray(models_with_metatada_but_no_data)

		metadata := MetadataJSON{ID: "MD:matsGui:" + name + ":COMMON:V01", Name: name, App: app}
		metadata.Updated = 0

		for i, m := range models {
			model := Model{Name: m}
			thresholds := getDistinctThresholds(conn, name, app, doctype, subDocType, m)
			log.Println(thresholds)
			fcstLen := getDistinctFcstLen(conn, name, app, doctype, subDocType, m)
			log.Println(fcstLen)
			region := getDistinctRegion(conn, name, app, doctype, subDocType, m)
			log.Println(region)
			displayText := getDistinctDisplayText(conn, name, app, doctype, subDocType, m)
			log.Println(displayText)
			displayCategory := getDistinctDisplayCategory(conn, name, app, doctype, subDocType, m)
			log.Println(displayCategory)
			displayOrder := getDistinctDisplayOrder(conn, name, app, doctype, subDocType, m, i)
			log.Println(displayOrder)
			minMaxCountFloor := getMinMaxCountFloor(conn, name, app, doctype, subDocType, m)
			log.Println(jsonPrettyPrintStruct(minMaxCountFloor[0].(map[string]interface{})))

			// ./sqls/getDistinctThresholds.sql returns list of variables for SUMS DocType, like in Surface
			if doctype == "SUMS" {
				model.Variables = thresholds
			} else {
				model.Thresholds = thresholds
			}
			model.Model = models[i]
			model.FcstLens = fcstLen
			model.Regions = region
			model.DisplayText = displayText[0]
			model.DisplayCategory = displayCategory[0]
			model.DisplayOrder = displayOrder[0]
			model.Mindate = int(minMaxCountFloor[0].(map[string]interface{})["mindate"].(float64))
			model.Maxdate = int(minMaxCountFloor[0].(map[string]interface{})["maxdate"].(float64))
			model.Numrecs = int(minMaxCountFloor[0].(map[string]interface{})["numrecs"].(float64))
			metadata.Updated = int(minMaxCountFloor[0].(map[string]interface{})["updated"].(float64))
			metadata.Models = append(metadata.Models, model)
		}
		log.Println(jsonPrettyPrintStruct(metadata))
		writeMetadataToDb(conn, metadata)
	*/
}
