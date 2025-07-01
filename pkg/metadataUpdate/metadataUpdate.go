package metadataUpdate

import (
	"fmt"
	"log"
	"time"

	"golang.org/x/exp/maps"

	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/types"
	"github.com/dtcenter/METjson2db/pkg/utils"
	// "github.com/couchbase/gocb/v2"
)

type StrArray []string

// init runs before main() is evaluated
func MetadataUpdate() {
	start := time.Now()
	log.Print("metadataUpdate:main()")

	conn := utils.GetDbConnection(state.Credentials)

	for ds := 0; ds < len(state.LoadSpec.Metadata); ds++ {
		updateMedataForAppDocType(conn, state.LoadSpec.Metadata[ds].Name, state.LoadSpec.Metadata[ds].App,
			state.LoadSpec.Metadata[ds].SubType, state.LoadSpec.Metadata[ds].LineType, state.LoadSpec.Metadata[ds].Version)
	}
	log.Printf("\tmeta update finished in %v", time.Since(start))
}

func updateMedataForAppDocType(conn types.CbConnection, name string, app string, subType string, lineType string, version string) {
	log.Println("updateMedataForAppDocType(" + name + "," + subType + "," + version + ")")

	// get all datasets in db
	datasets := GetDatasets(conn, app, subType, lineType)
	log.Println("datasets:")
	utils.PrintStringArray(datasets)

	metadata := types.Metadata{ID: "MD:METexpressGui:met-cyclone:" + version, Type: "MD", DocType: "METexpressGui", App: "met-cyclone", Version: state.LoadSpec.Version, Generated: true}

	for dsi := 0; dsi < len(datasets); dsi++ {
		models := GetModels(conn, app, subType, version, datasets[dsi])
		log.Println("dataset:" + datasets[dsi] + ",models:")
		utils.PrintStringArray(models)
		dataset := types.Dataset{Dataset: datasets[dsi]}

		for mi := 0; mi < len(models); mi++ {
			// lineTypes := GetLineTypes(conn, app, subType, version, datasets[dsi], models[mi])
			log.Println("dataset:" + datasets[dsi] + ",model:" + models[mi])
			// utils.PrintStringArray(lineTypes)
			model := types.Model{Model: models[mi]}

			// for lti := 0; lti < len(lineTypes); lti++ {
			basins := GetBasins(conn, app, subType, version, datasets[dsi], models[mi], lineType)
			log.Println("dataset:" + datasets[dsi] + ",model:" + models[mi] + ",lineType:" + lineType + ",basins:")
			utils.PrintStringArray(basins)
			linetype := types.LineType{LineType: lineType}

			for bi := 0; bi < len(basins); bi++ {
				stormIds := GetStormIDs(conn, app, subType, version, datasets[dsi], models[mi], lineType, basins[bi])
				log.Println("dataset:" + datasets[dsi] + ",model:" + models[mi] + ",lineType:" + lineType + ",basin:" + basins[bi] + ",stormIds:")
				utils.PrintStringArray(stormIds)
				basin := types.Basin{Basin: basins[bi]}

				year_to_stormids_map := make(map[string][]string)
				for sti := 0; sti < len(stormIds); sti++ {
					s := stormIds[sti]
					year := s[len(s)-4:]
					year_to_stormids_map[year] = append(year_to_stormids_map[year], s)
				}

				for year, stormids := range year_to_stormids_map {
					fmt.Printf("Key: %s, Value: %v\n", year, stormids)
					docsForStormIds := GetDocsForStormIds(conn, app, subType, version, datasets[dsi], models[mi], lineType, basins[bi], stormids)
					fmt.Printf("docsForStormIds:%d\n%s", len(docsForStormIds), utils.JsonPrettyPrintStruct(docsForStormIds))
					stormid := types.StormId{Year: year}

					storms := make(map[string]bool)
					truths := make(map[string]bool)
					levels := make(map[string]bool)
					fcst_lens := make(map[string]bool)
					numrecs := 0
					mindate := float64(0)
					maxdate := float64(0)

					for i := 0; i < len(docsForStormIds); i++ {
						doc := docsForStormIds[i].(map[string]interface{})
						storms[doc["STORM_ID"].(string)+"-"+doc["STORM_NAME"].(string)] = true
						truths[doc["BMODEL"].(string)] = true
						// descriptions[doc["DECR"].(string)] = true
						data := doc["data"].(map[string]interface{})
						numrecs = numrecs + len(maps.Keys(data))

						if mindate == 0 || doc["VALID"].(float64) < mindate {
							mindate = doc["VALID"].(float64)
						}
						if maxdate == 0 || doc["VALID"].(float64) > maxdate {
							maxdate = doc["VALID"].(float64)
						}

						for dataKey, dv := range data {
							fcst_lens[dataKey] = true
							dataVal := dv.(map[string]interface{})
							if nil != dataVal["level"] {
								level := dataVal["level"].(string)
								levels[level] = true
							}
						}
					}
					stormid.MdCounts.Storms = maps.Keys(storms)
					stormid.MdCounts.Truths = maps.Keys(truths)
					stormid.MdCounts.Levels = maps.Keys(levels)
					stormid.MdCounts.FcstLens = maps.Keys(fcst_lens)
					stormid.MdCounts.Numrecs = numrecs
					stormid.MdCounts.Mindate = mindate
					stormid.MdCounts.Maxdate = maxdate
					stormid.MdCounts.Updated = float64(time.Now().UTC().Unix())

					basin.StormIds = append(basin.StormIds, stormid)
				}
				linetype.Basins = append(linetype.Basins, basin)
			}
			model.LineTypes = append(model.LineTypes, linetype)
			// }
			dataset.Models = append(dataset.Models, model)
		}
		metadata.Datasets = append(metadata.Datasets, dataset)
	}
	fmt.Println(utils.JsonPrettyPrintStruct(metadata))

	writeMetadataToDb(conn, metadata)
	log.Println("Metadata ID:" + metadata.ID)
}

func writeMetadataToDb(conn types.CbConnection, metadata types.Metadata) {
	_, err := conn.Collection.Upsert(metadata.ID, metadata, nil)
	if err != nil {
		log.Fatal(err)
	}
}
