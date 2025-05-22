package types

import (
	"github.com/couchbase/gocb/v2"
)

// type CbDataDocument map[string]interface{}
type CbConnection struct {
	Cluster    *gocb.Cluster
	Bucket     *gocb.Bucket
	Scope      *gocb.Scope
	Collection *gocb.Collection
	VxDBTARGET string
}

type StrArray []string

type MdCounts struct {
	Storms       StrArray `json:"storms"`
	Truths       StrArray `json:"truths"`
	Descriptions StrArray `json:"descriptions"`
	FcstLens     StrArray `json:"fcstLens"`
	Levels       StrArray `json:"levels"`
	Mindate      float64  `json:"mindate"`
	Maxdate      float64  `json:"maxdate"`
	Numrecs      int      `json:"numrecs"`
	Updated      float64  `json:"updated"`
}

type StormId struct {
	Year     string   `json:"year"`
	MdCounts MdCounts `json:"mdcounts"`
}

type Basin struct {
	Basin    string    `json:"basin"`
	StormIds []StormId `json:"years"`
}

type LineType struct {
	LineType string  `json:"linetype"`
	Basins   []Basin `json:"basins"`
}

type Model struct {
	Model     string     `json:"model"`
	LineTypes []LineType `json:"linetypes"`
}

type Dataset struct {
	Dataset string  `json:"dataset"`
	Models  []Model `json:"models"`
}

type Metadata struct {
	ID        string    `json:"id"`
	App       string    `json:"app"`
	Type      string    `json:"type"`
	DocType   string    `json:"docType"`
	Version   string    `json:"version"`
	Generated bool      `json:"generated"`
	Datasets  []Dataset `json:"datasets"`
}

type LoadSpec struct {
	Version                       string   `json:"version"`
	Email                         string   `json:"email"`
	TargetCollection              string   `json:"targetCollection"`
	DatasetName                   string   `json:"datasetName"`
	Verbose                       bool     `json:"verbose"`
	LogLevel                      string   `json:"logLevel"`
	RunMode                       string   `json:"runMode"`
	JsonArchiveFilePathAndPrefix  string   `json:"jsonArchiveFilePathAndPrefix"`
	MaxDocIdLength                int64    `json:"maxDocIdLength"`
	ProgressiveLargeDocIdPolicies []string `json:"progressiveLargeDocIdPolicies"`
	OverWriteData                 bool     `json:"overWriteData"`
	RunNonThreaded                bool     `json:"runNonThreaded"`
	ThreadsDbUpload               int64    `json:"threadsDbUpload"`
	ThreadsMergeDocFetch          int64    `json:"threadsMergeDocFetch"`
	ChannelBufferSizeNumberOfDocs int64    `json:"channelBufferSizeNumberOfDocs"`
	FolderTmpl                    string   `json:"folderTmpl"`
	Metadata                      []struct {
		Name     string `json:"name"`
		App      string `json:"app"`
		SubType  string `json:"subType"`
		LineType string `json:"linetype"`
		Version  string `json:"version"`
	} `json:"metadata"`
	LoadVal struct {
		Field []struct {
			Val  StrArray `json:"val"`
			Name string   `json:"name"`
		} `json:"field"`
	} `json:"loadVal"`
	LoadNote string `json:"loadNote"`
}

type Credentials struct {
	Cb_host       string `yaml:"cb_host"`
	Cb_user       string `yaml:"cb_user"`
	Cb_password   string `yaml:"cb_password"`
	Cb_bucket     string `yaml:"cb_bucket"`
	Cb_scope      string `yaml:"cb_scope"`
	Cb_collection string `yaml:"cb_collection"`
}

type LineTypeStat struct {
	ProcessedCount int  `json:"processedCount"`
	Handled        bool `json:"handled"`
}

type DocKeyCounts struct {
	HeaderLen int
	DataLen   int
}

type StatToCbRun struct {
	FileStatus map[string]string      // filename:status
	Documents  map[string]interface{} // id:doc
}
