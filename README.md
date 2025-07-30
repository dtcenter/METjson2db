# METjson2db

## Version v0.1.0

# MET stat files to Couchbase JSON document converter and uploader
This project is a GO language command-line program to generate Couchbase JSON documents 
and/or upload these documents to Couchbase database.
 
## Inline upload vs JSON archive generation
There are multiple modes in which this program can be run. 
The run mode is specified in the load_spec.json file.

NOTE: run mode specified in load_spec.json can be overidden in
the command line, for example:
-m METADATA_UPDATE

Refer to load_spec documentation for a detailed description of these run modes.

The current run modes are: 
"runMode" : "DIRECT_LOAD_TO_DB"
"runMode" : "CREATE_JSON_DOC_ARCHIVE"
"runMode" : "METADATA_UPDATE"

Please note that the setting:
"overWriteData": false
which enables merge functionality, wherein incoming data is merged with data already in the database
is only available in run mode:
"runMode" : "DIRECT_LOAD_TO_DB"

## The 2-step process
### STEP 1 : Generate line type definitions for a particular MET version
This step needs to be done once for each MET version, and the genrerated code checked in to METstat2json git repo:main
Refer to documentation section "Generating LINE_TYPE definitions" for details on how to do this
### STEP 2 : Run METdadacb to generate and/or upload JSON documents to Couchbase/Capella
See section "Running the METjson2db to generate and/or upload JSON documents"

# Couchbase
Unlike relational databases like MySQL, Postgres etc, Couchbase is a JSON document based database where
the unit of storage is a single JSON document. This model is much more flexible than a relational model, 
for example, you can add another attribute to your documents without adding/modifying a table schema.  
In fact there are no tables or table metadata like columns, column types etc that needs to be maintained and
kept in sync with data. 

## Couchbase data organisation
### Buckets, Scopes, Collections
Unlike relational databases, Couchbase follows a simple hierarchical model of
Buckets=>Scopes=>Collections
See this link for a detailed descrion:
https://docs.couchbase.com/cloud/clusters/data-service/about-buckets-scopes-collections.html 

For MET data, once this hierrarchy is created in the database server (see section installing and configuring Couchbase)
the information needs to be configured in a credentials file, that lives external to github repository for security reasons.
The MET data uploader defaults to ~/credentials for this file, but can be overridden in the command line.
See METjson2db/credentials.template for the content and syntax of this creadentials file.
The Collection itself can be overridden in the load_spec.json file, thus allowing for data to be stored in separate collections
for research and comparison purposes.

### Queries and indexes
Couchbase queries are similar to SQL with some additional sematics for dealing with JSON data,and is called SQL++
See this link for SQL++ documentation:
https://docs.couchbase.com/server/current/n1ql/query.html 
Indexes serve similar purpose that in relational databases, namely, to speed up specific queries.
SQL++ statements for creating the required indexes are in : METjson2db/indexes
See see section installing and configuring Couchbase for more information on creating indexes

## MET Couchbase Document format (data model)
METplus output can generally be split into “header” fields, which describe the context of the statistics being generated, and
“data” fields, which provide the actual statistics. For example, header fields could include parameters such as model, threshold, 
region mask, etc, while data fields would include partial sums, contingency table counts, etc. In this couchbase METplus document 
design, header fields are stored at the top level of the document, while data fields are stored in a “data” section, keyed by 
forecast lead or some other delimiting value, like object ID for MODE data.
### Indexes
Depending on what kind of queries are required, one or more of the header field can be used to create an index.
See .sql files in METjson2db/indexes for examples of index creation SQL scripts.

# Installing and configuring Couchbase for METjson2db
1. Have your system administrator install Couchbase Enterperise, Community or the Cloud managed service Capella
2. Obtain the Web UI to Couchbase from them and admin credentials
3. Log into Couchbase Web UI using admin credentials
The following examples will be referencing a local Couchbase server at GSL (Enterprise Edition 7.6.2), and its web UI.
Please note that instructions may be different for another Couchbase version. Please refer to Couchbase documentation
for the correct instructions for your version.
https://docs.couchbase.com/server/current/introduction/whats-new.html 
Web UI URL for GSL on-prem Couchbase: 
http://adb-cb1.gsd.esrl.noaa.gov:8091/ui/index.html
## Create a non-admin user
1. Open Couchbase Web UI using URL you obtained from your sys admin, in your browser of choice.
NOTE: You may have to open this in an incognito window in some certificate issue situations.
2. Login as Administrator.
The main URL takes you to the dashboard
## Create Bucket, Scope and Collection(s)
1. Open Couchbase Web UI using URL you obtained from your sys admin, in your browser of choice.
NOTE: You may have to open this in an incognito window in some certificate issue situations.
2. Login as Administrator.
The main URL takes you to the dashboard
3. On the left controls, click on "Buckets"
4. On the top-right corner, click on "ADD BUCKET"
5. Give bucket name, example: "metplusdata" (accept defaults for everyting else)
6. Click on "Scopes and Collections" 
7. Click on "_default" scope and then click on "Add Collection"
8. Give collection a name, example: MET_default
9. Edit your ~/credentials file and put the newly created bucket name,collection name, non-admin username and password in it.
10. Edit your load_spec.json, and put collection name in it. NOTE: Collection name specified in ~/credentials can be 
overridden in the load_spec.json file

## Create required indexes
All the index creation SQL scripts are in METjson2db/indexes
Most, if not all, of these index scripts are required by specific MET apps, so this list will grow as more MET Couchbase apps are added.
## Couchbase index adviser
Couchbase has an index adviser.  The best way to use this is to run a query in the web UI. When a query is run, Couchbase will 
generate an index advice which will list, new indexes, if any, that may be needed to speed up the query.

# Generating LINE_TYPE definitions
# This needs to be done once for each MET version change, OR when LINE_TYPE definitions change in the following MET repo files
git clone https://github.com/dtcenter/METstat2json
make sure you are in the main branch
cd  <repo path>/pkg/buildHeaderLineTypes
go run . > /tmp/types.go
cp /tmp/types.go ../structColumnTypes/structColumnTypes.go
Generated definitions are in: <repo path>/pkg/structColumnTypes/structColumnTypes.go
Once the line type definitions are thus updated using the steps above, changes must be 
checked into main branch, so that METjson2db will reference the right METstat2json source files

## Running the METjson2db to generate and/or upload JSON documents 
Make sure of the following:
1. METjson2db is configured as detailed in section "Configuration "load_spec.json"
2. A default credential file exists in your home folder ~/credentials or provide one on command line
3. Modify load_spec.json to match your input data files, or use "recursive with file pattern regex match", as given
in examples below.

# METjson2db runtime state setup
METjson2db runtime state is controlled by the following, each of which is described in detail in respective sections.
1. Credentials file
2. load_spec file
3. Command line parameters

## Credentials file, which defaults to ~/credentials
This file sets the following:
1. Couchbase URL, bucket, scope and default collection
2. Connection username and password 
A sample credentials file is available in: METjson2db/credentials.template

## load_spec file, which defaults to "./load_spec.json"
Unless overridden in the command line, the default file is 'load_spec.json' in the METdadacb run folder.

### logLevel   
Options are: ["DEBUG", "INFO", "WARN", "ERROR"]
This sets the METdadacb log level. Please set to ERROR for production

### runMode
Options are:

DIRECT_LOAD_TO_DB - METdadacb will load input stat files in-line, at run time, to the Couchbase database.
Please note that the merge mode, when set using [overWriteData: false], is only available in DIRECT_LOAD_TO_DB mode.

CREATE_JSON_DOC_ARCHIVE - METdadacb will create a gzip archive of Couchbase json documents from input stat files,
which can then be uploaded to Couchbase later using a cbimport command line tool.
Please note that the merge mode, when set using [overWriteData: false], is NOT available in CREATE_JSON_DOC_ARCHIVE mode,
since the cbimport tool will overwrite existing documents in the database that has same ID as incoming documents.
In this mode, the setting:
"jsonArchiveFilePathAndPrefix" :"/scratch/METjson2db_out_"
sets the folder and file prefix for the generated archive file.  At the end of a succesfull run, the output file name 
will looks like below (prefix + timestamp):
/scratch/METjson2db_out_2025-04-09T14:40:11-06:00

METADATA_UPDATE - METdatacb will update the metadata in the database from currently existing data documents in the database.

### maxDocIdLength
As of Couchbase version 7.6.2, the max length of document ID is 250 characters.
The document IDs are generated from the header fields of a document, so this will
cause an error to be flagged in any ID exceeds this setting.

### overWriteData
NOTE: Only applies in runMode = DIRECT_LOAD_TO_DB
If overWriteData = false, this will trigger a database merge mode.
In merge mode, before a document is uploaded to the database, if a document exists in the database with the same ID,
that document is fetched from the database, the a merged with the incoming document as per logic below:
a) Header fields which exist in the database document, but missing from incoming document, it is added to incoming document
b) If a data section key exists in the database document, but missing from incoming document, it is added to incoming documentß

### runNonThreaded
NOTE: Only applies in runMode = DIRECT_LOAD_TO_DB
The default run mode is multi-threaded, where multiple concurrent database connections are used for inserting documents to the databse
If  runNonThreaded is set to true, the runtime will be single threaded.  This is mainly only useful in debugging the code.

### threadsDbUpload
NOTE: Only applies in runMode = DIRECT_LOAD_TO_DB and runNonThreaded = false
Specifies the number of database upload threads

### threadsMergeDocFetch
NOTE: Only applies in runMode = DIRECT_LOAD_TO_DB and runNonThreaded = false and overWriteData = false
Specifies the number of concurrent threads used to fetch existing documents from database for merge

### channelBufferSizeNumberOfDocs
GO uses channels to ficilitate data transfer to threads.  This sets the max buffer size for each such channel.

## Command line parameters
The following command line parameters, if supplied, overrides what is set in load_spec
-c : credentials file
-l : load_spec file
-m : run mode
-d : data set name
-f : single stat file to process
-i : process all files in this folder
-I : process all files in this folder tree (recursive)
-r : file name match regex


### METjson2db run sample commands
Currently the following works:

```shell
# Run all packages in `cmd/`
# THERE MUST BE A Collection named MET_tests
go run ./cmd/...
# Run all tests with a coverage report
go clean -testcache
go test -cover ./...
go test -v ./...

# And some tooling examples

# Format code inplace, apply simplifications if possible, and show the diff
gofmt -w -s -d .
# Run static analysis
go vet ./...
# Tidy up dependencies
go mod tidy
# Build the "test" binary
go build -o /tmp/test ./cmd/test
# Run various Linters used in CI
brew install golangci-lint # If not installed already
golangci-lint run
```

```shell
cd METjson2db
go build .
# run using ~/credentials, ./load_spec.json 
go run ./cmd/... -c ~/credentials -l ./load_spec.json
# run on adb-cb1 (with non-default credentials file)
go run ./cmd/... -c ~/credentials.MET
# to test for race conditions, add "-race" as below
go run -race ./cmd/... -c ~/credentials -l ./load_spec.json
# run with specific credentials, load_spec and/or for a specific stat file
go run ./cmd/... -c ~/credentials -l ./load_spec.json -f /Users/gopa.padmanabhan/scratch/data/MET/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.stat
# if -f option is specified, ignores load_spec input files

go run ./cmd/... -c ~/credentials -l ./load_spec.json -f ./test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_240000L_20240203_120000V.stat

# Output will be in ./outputs with file name with extension as json, like: grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.json

# run with specific credentials,settings and/or for all stat files in a folder
go run ./cmd/... -c ~/credentials -l ./load_spec.json -i ./test_data/
go run ./cmd/... -c ~/credentials -l ./load_spec.json -i /Users/gopa.padmanabhan/scratch/data/MET/G2G_v12/G2G_v12/20241104-06z/grid_stat
go run ./cmd/... -c ~/credentials -l ./load_spec.json -i /Users/gopa.padmanabhan/scratch/data/MET/
# recursive with file pattern regex match
go run ./cmd/... -c ~/credentials -l ./load_spec.json -I /Users/gopa.padmanabhan/scratch/data/MET/tc_data/tc_data/ -r ".tcst"
go run ./cmd/... -c ~/credentials.MET -d gopa01 -l ./load_spec.json -I /home/amb-verif/MET_data/tc_data/ -r ".tcst"
# if -f,-F,-i OR -I options are specified, ignores load_spec input files

# update metadata
go run ./cmd/... -c ~/credentials -l ./load_spec.json -m METADATA_UPDATE
```


## Log output to file
By default, log is printed to stdout, but if you instead want to log to a file, add below at the end of above run commands:
2> logfile (for overwriting)
2>> logfile (for appending)

