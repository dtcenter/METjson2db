package types

import (
	"github.com/couchbase/gocb/v2"
)

//type CbDataDocument map[string]interface{}

type CbConnection struct {
	Cluster    *gocb.Cluster
	Bucket     *gocb.Bucket
	Scope      *gocb.Scope
	Collection *gocb.Collection
}

type StrArray []string

type Metadata []struct {
	Name       string   `json:"name"`
	App        string   `json:"app"`
	SubDocType string   `json:"subDocType"`
	DocType    StrArray `json:"docType"`
}

type LoadSpec struct {
	Email                         string   `json:"email"`
	TargetCollection              string   `json:"target_collection"`
	DatasetName                   string   `json:"dataset_name"`
	Verbose                       bool     `json:"verbose"`
	LogLevel                      string   `json:"LogLevel"`
	RunMode                       string   `json:"runMode"`
	JsonArchiveFilePathAndPrefix  string   `json:"jsonArchiveFilePathAndPrefix"`
	MaxDocIdLength                int64    `json:"maxDocIdLength"`
	ProgressiveLargeDocIdPolicies []string `json:"progressiveLargeDocIdPolicies"`
	OverWriteData                 bool     `json:"overWriteData"`
	RunNonThreaded                bool     `json:"runNonThreaded"`
	ThreadsDbUpload               int64    `json:"threadsDbUpload"`
	ThreadsMergeDocFetch          int64    `json:"threadsMergeDocFetch"`
	ChannelBufferSizeNumberOfDocs int64    `json:"channelBufferSizeNumberOfDocs"`
	FolderTmpl                    string   `json:"folder_tmpl"`
	LoadVal                       struct {
		Field []struct {
			Val  StrArray `json:"val"`
			Name string   `json:"_name"`
		} `json:"field"`
	} `json:"load_val"`
	LoadNote string `json:"load_note"`
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
	ProcessedCount int  `json:"ProcessedCount"`
	Handled        bool `json:"Handled"`
}

type DocKeyCounts struct {
	HeaderLen int
	DataLen   int
}

type StatToCbRun struct {
	FileStatus map[string]string      // filename:status
	Documents  map[string]interface{} // id:doc
}
