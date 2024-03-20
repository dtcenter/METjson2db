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

type ConfigJSON struct {
	Metadata []struct {
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
	}
	if len(inputFolder) > 0 {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",inputFolder:" + inputFolder)
	}

	/*
		conf := ConfigJSON{}

		conf, err := parseConfig(settingsFilePath)
		if err != nil {
			log.Fatal("Unable to parse config")
			return
		}
	*/

	// credentials := getCredentials(credentialsFilePath)

	// conn := getDbConnection(credentials)

	log.Println(fmt.Sprintf("\tstatToCbDoc finished in %v", time.Since(start)))
}

func parseConfig(file string) (ConfigJSON, error) {
	log.Println("parseConfig(" + file + ")")

	conf := ConfigJSON{}
	configFile, err := os.Open(file)
	if err != nil {
		log.Print("opening config file", err.Error())
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
