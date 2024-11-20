package types

import (
	"github.com/couchbase/gocb/v2"
)

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
	Email            string `json:"email"`
	TargetCollection string `json:"target_collection"`
	Verbose          bool   `json:"verbose"`
	FolderTmpl       string `json:"folder_tmpl"`
	LoadVal          struct {
		Field []struct {
			Val  StrArray `json:"val"`
			Name string   `json:"_name"`
		} `json:"field"`
	} `json:"load_val"`
	LoadNote string `json:"load_note"`
}

type Column struct {
	Name string
	Type string
}

type LineType struct {
	LineType string
	Columns  []Column
}

type DefSingleTypeColumns struct {
	Names []string `json:"names"`
	Type  string   `json:"type"`
}

type DefLineType struct {
	LineType string                 `json:"lineType"`
	Columns  []DefSingleTypeColumns `json:"columns"`
}

type ConfigJSON struct {
	MaxLinesToLoad                 int64    `json:"maxLinesToLoad"`
	MaxFilesInProcessChunk         int64    `json:"maxFilesInProcessChunk"`
	UpdateOnlyOnDocKeyCountChange  bool     `json:"updateOnlyOnDocKeyCountChange"`
	FlushToDbDataSectionMaxCount   int64    `json:"flushToDbDataSectionMaxCount"`
	OverWriteData                  bool     `json:"overWriteData"`
	WriteJSONsToFile               bool     `json:"writeJSONsToFile"`
	UploadToDb                     bool     `json:"uploadToDb"`
	OutputFolder                   string   `json:"outputFolder"`
	RunNonThreaded                 bool     `json:"runNonThreaded"`
	ThreadsFileProcessor           int64    `json:"threadsFileProcessor"`
	ThreadsWriteToDisk             int64    `json:"threadsWriteToDisk"`
	ThreadsDbUpload                int64    `json:"threadsDbUpload"`
	ChannelBufferSizeNumberOfDocs  int64    `json:"channelBufferSizeNumberOfDocs"`
	ChannelBufferSizeNumberOfFiles int64    `json:"channelBufferSizeNumberOfFiles"`
	LineTypeDefs                   string   `json:"lineTypeDefs"`
	IdColumns                      []string `json:"idColumns"`
	HeaderColumns                  []string `json:"headerColumns"`
	DataKeyColumns                 []string `jaon:"dataKeyColumns"`
	IgnoreColumns                  []string `json:"ignoreColumns"`
	IgnoreValues                   []string `json:"ignoreValues"`
	CommonColumns                  []Column `json:"commonColumns"`
	LineTypeColumns                []LineType
}

type TroubleShoot struct {
	EnableTrackContextFlushToFile bool `json:"enableTrackContextFlushToFile"`
	EnableTrackContextFlushToDb   bool `json:"enableTrackContextFlushToDb"`
	EnableLineTypeTrack           bool `json:"enableLineTypeTrack"`
	TerminateAtFirstTrackError    bool `json:"terminateAtFirstTrackError"`
	IdTrack                       struct {
		IdList  []string `json:"idList"`
		Actions []string `json:"actions"`
	} `json:"idTrack"`
	LineTypeTrack struct {
		LineTypeList []string `json:"lineTypeList"`
		Actions      []string `json:"actions"`
	} `json:"lineTypeTrack"`
}

type Credentials struct {
	Cb_host       string `yaml:"cb_host"`
	Cb_user       string `yaml:"cb_user"`
	Cb_password   string `yaml:"cb_password"`
	Cb_bucket     string `yaml:"cb_bucket"`
	Cb_scope      string `yaml:"cb_scope"`
	Cb_collection string `yaml:"cb_collection"`
}

// the map below holds template docs created from settings.json
type ColDef struct {
	Name      string
	DataType  int // 0-string, 1-int64, 2-float64, 3-epoch
	IsHeader  bool
	IsID      bool
	IsDataKey bool
}

type ColDefArray []ColDef

type LineTypeStat struct {
	ProcessedCount int  `json:"ProcessedCount"`
	Handled        bool `json:"Handled"`
}

type DocKeyCounts struct {
	HeaderLen int
	DataLen   int
}

type StatToCbRun struct {
	FileStatus map[string]string         // filename:status
	Documents  map[string]CbDataDocument // id:doc
}
