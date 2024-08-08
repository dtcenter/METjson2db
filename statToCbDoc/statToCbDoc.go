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

type Metadata []struct {
	Name       string   `json:"name"`
	App        string   `json:"app"`
	SubDocType string   `json:"subDocType"`
	DocType    StrArray `json:"docType"`
}

type LoadSpec struct {
	Email        string `json:"email"`
	InitializeDb bool   `json:"initialize_db"`
	Organization string `json:"organization"`
	Verbose      bool   `json:"verbose"`
	InsertSize   int64  `json:"insert_size"`
	FolderTmpl   string `json:"folder_tmpl"`
	LoadVal      struct {
		Field []struct {
			Val  StrArray `json:"val"`
			Name string   `json:"_name"`
		} `json:"field"`
	} `json:"load_val"`
	LoadNote string `json:"load_note"`
}

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type ConfigJSON struct {
	MaxLinesToLoad   int64    `json:"maxLinesToLoad"`
	WriteJSONsToFile bool     `json:"writeJSONsToFile"`
	HeaderColumns    []string `json:"headerColumns"`
	CommonColumns    []Column `json:"commonColumns"`
	LineTypeColumns  []struct {
		LineType string   `json:"lineType"`
		Columns  []Column `json:"columns"`
	}
}

type Credentials struct {
	Cb_host       string `yaml:"cb_host"`
	Cb_user       string `yaml:"cb_user"`
	Cb_password   string `yaml:"cb_password"`
	Cb_bucket     string `yaml:"cb_bucket"`
	Cb_scope      string `yaml:"cb_scope"`
	Cb_collection string `yaml:"cb_collection"`
}

// var builders map[string]IStatToCbBuilder

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

	// builders = make(map[string]IStatToCbBuilder)

	home, _ := os.UserHomeDir()
	var credentialsFilePath string
	flag.StringVar(&credentialsFilePath, "c", home+"/credentials", "path to credentials file")

	var settingsFilePath string
	flag.StringVar(&settingsFilePath, "s", "./settings.json", "path to settings.json")

	var loadSpecFilePath string
	flag.StringVar(&loadSpecFilePath, "l", "./load_spec.json", "path to load_spec.json")

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
	fmt.Println("maxLinesToLoad:", conf.MaxLinesToLoad)
	fmt.Println("writeJSONsToFile:", conf.WriteJSONsToFile)
	fmt.Println("HeaderColumns length:", len(conf.HeaderColumns))
	fmt.Println("CommonColumns length:", len(conf.CommonColumns))
	fmt.Println("LineTypeColumns length:", len(conf.LineTypeColumns))

	loadSpec, err := parseLoadSpec(loadSpecFilePath)
	if err != nil {
		log.Fatal("Unable to parse config")
		return
	}
	fmt.Println("folder_tmpl:", len(loadSpec.FolderTmpl))
	fmt.Println("LoadVal.Field[0].Val length:", len(loadSpec.LoadVal.Field[0].Val))

	var files []string
	if len(inputFile) > 0 {
		files = append(files, inputFile)
		startProcessing(files)
	}

	// credentials := getCredentials(credentialsFilePath)

	// conn := getDbConnection(credentials)

	testDoc := createTestcbDocument()
	fmt.Println("testDoc:", testDoc.toJSONString())

	log.Printf("\tstatToCbDoc finished in %v", time.Since(start))
}

func parseLoadSpec(file string) (LoadSpec, error) {
	log.Println("parseLoadSpec(" + file + ")")

	ls := LoadSpec{}
	configFile, err := os.Open(file)
	if err != nil {
		log.Fatal("opening load_spec file", err.Error())
		configFile.Close()
		return ls, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&ls); err != nil {
		log.Fatalln("parsing load_spec file", err.Error())
		return ls, err
	}

	return ls, nil
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
