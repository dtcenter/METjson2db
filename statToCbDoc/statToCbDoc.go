package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
	// "github.com/couchbase/gocb/v2"
)

type StrArray []string
type LineTypeColumn struct {
	LineType string   `json:"lineType"`
	Fields   StrArray `json:"fields"`
}

type FieldMapElement struct {
	MET_name  string `json:"MET_name"`
	MATS_name string `json:"MATS_name"`
}

type ConfigJSON struct {
	CommonColumns   StrArray          `json:"commonColumns"`
	LineTypeColumns []LineTypeColumn  `json:"lineTypeColumns"`
	FieldMap        []FieldMapElement `json:"fieldMap"`
	Metadata        []struct {
		Name       string   `json:"name"`
		App        string   `json:"app"`
		SubDocType string   `json:"subDocType"`
		DocType    StrArray `json:"docType"`
	} `json:"metadata"`
}

type Credentials struct {
	Cb_host       string `yaml:"cb_host"`
	Cb_user       string `yaml:"cb_user"`
	Cb_password   string `yaml:"cb_password"`
	Cb_bucket     string `yaml:"cb_bucket"`
	Cb_scope      string `yaml:"cb_scope"`
	Cb_collection string `yaml:"cb_collection"`
}

var builders map[string]IStatToCbBuilder

// init runs before main() is evaluated
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("meta-update:init()")
}

func main() {
	// Uncomment following line to enable logging
	// gocb.SetLogger(gocb.VerboseStdioLogger())

	start := time.Now()
	log.Print("meta-update:main()")

	builders = make(map[string]IStatToCbBuilder)

	home, _ := os.UserHomeDir()
	var credentialsFilePath string
	flag.StringVar(&credentialsFilePath, "c", home+"/credentials", "path to credentials file")

	var settingsFilePath string
	flag.StringVar(&settingsFilePath, "s", "./settings.json", "path to settings.json file")

	var inputFile string
	flag.StringVar(&inputFile, "f", "", "stat file full path")

	var inputFolder string
	flag.StringVar(&inputFolder, "i", "./input_files", "input stat files folder")

	flag.Parse()

	if len(inputFile) > 0 {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",inputFile:" + inputFile)
		statFileToCbDoc(inputFile)
	} else if len(inputFolder) > 0 {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",inputFolder:" + inputFolder)
	} else {
		log.Fatal("Must specify either an input file (-f) or an input folder (-i)!")
		os.Exit(1)
	}

	conf := ConfigJSON{}
	conf, err := parseConfig(settingsFilePath)
	if err != nil {
		log.Fatal("Unable to parse config")
		return
	}
	fmt.Println("CommonColumns length:", len(conf.CommonColumns))
	fmt.Println("LineTypeColumns length:", len(conf.LineTypeColumns))
	fmt.Println("FieldMap length:", len(conf.FieldMap))

	if len(inputFile) > 0 {
		statFileToCbDoc(inputFile)
	}

	// credentials := getCredentials(credentialsFilePath)

	// conn := getDbConnection(credentials)

	log.Printf("\tstatToCbDoc finished in %v", time.Since(start))
}

func parseConfig(file string) (ConfigJSON, error) {
	log.Println("parseConfig(" + file + ")")

	conf := ConfigJSON{}
	configFile, err := os.Open(file)
	if err != nil {
		log.Fatal("opening config file", err.Error())
		configFile.Close()
		return conf, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&conf); err != nil {
		log.Fatalln("parsing config file", err.Error())
		return conf, err
	}

	return conf, nil
}

func getCredentials(credentialsFilePath string) Credentials {
	creds := Credentials{}
	yamlFile, err := os.ReadFile(credentialsFilePath)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, &creds)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	return creds
}
