# METdatacb

## MET stat file to Couchbase JSON document converter and uploader

This project is a GO language command-line program to generate and/or upload MET
stat file data to Couchbase JSON documents.  

# Purpose
The purpose of METdadacb is to generate and optionally upload JSON documents conforming to MET Couchbase schema
frm MET stat files

# The 2-step process
STEP 1 : Generate line type definitions for a particular MET version
STEP 2 : Run METdadacb to generate and/or upload JSON documents to Couchbase/Capella

# Couchbase
Unlike relational databases like MySQL, PostGres etc, Couchbase is a JSON document based database where
the unit of storage is a single JSON document. This model is much more flexible than a relational model, 
for example, you can add another attribute to your documents without adding/modifying a table schema.  
In fact there are no tables or table metadata like columns, column types etc that needs to be maintained and
kept in sync with data. 

## Couchbase data organisation
### Buckets, Scopes, Collections
### Queries and indexes

# MET for Couchbase 
## Document format (data model)
Describe in brief about header & data sections, data key used

## Bucket, scope, collection configuration for METdatacb
Credentials and load_spec

# Installing and configuring Couchbase community edition for METdatacb







## Generating LINE_TYPE definitions
# This needs to be done once for each MET version change, OR when LINE_TYPE definitions change in the following MET repo files
git clone https://github.com/NOAA-GSL/MET-PARSER
make sure you are in the main branch
cd  <repo path>/pkg/buildHeaderLineTypes
go run . > /tmp/types.go
cp /tmp/types.go ../structColumnTypes/structColumnTypes.go
Generated definitions are in: <repo path>/pkg/structColumnTypes/structColumnTypes.go
Once the line type definitions are thus updated using the steps above, changes must be 
checked into main branch, so that METdatacb will reference the right MET-parser source files

## Running the METdatacb to generate and/or upload JSON documents 


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

## Configuration
### Couchbase credentials file
NOTE: If no specific credentails file is given on the command-line, statToCbDoc
will look for and use ~/credentials
There is a sample credentials file at: METdatacb/credentials.template

statToCbDoc picks up Couchbase conection information from this file, example below
Please note that the cb_user and cb_password values should be replaced with actual values
cb_host: couchbase://adb-cb1.gsd.esrl.noaa.gov
cb_user: ***
cb_password: ***
cb_bucket: metdata
cb_scope: _default
#### The target collection must be specified in load_spec.json => "target_collection": "MET_default"

To point to cluster, use
cb_host: adb-cb2.gsd.esrl.noaa.gov,adb-cb3.gsd.esrl.noaa.gov,adb-cb4.gsd.esrl.noaa.gov


## Example run command-lines
```shell
cd METdatacb
go build .
# run using ~/credentials, ./settings.json , ./load_spec.json 
go run ./cmd/... -c ~/credentials -s ./settings.json -l ./load_spec.json
# run on adb-cb1 (with non-default credentials file)
go run ./cmd/... -c ~/credentials.MET
# to test for race conditions, add "-race" as below
go run -race ./cmd/... -c ~/credentials -s ./settings.json -l ./load_spec.json
# run with specific credentials,settings, load_spec and/or for a specific stat file
go run ./cmd/... -c ~/credentials -s ./settings.json -l ./load_spec.json -f /Users/gopa.padmanabhan/scratch/data/MET/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.stat
# if -f option is specified, ignores load_spec input files

go run ./cmd/... -c ~/credentials -s ./settings.json -l ./load_spec.json -f ./test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_240000L_20240203_120000V.stat

# Output will be in ./outputs with file name with extension as json, like: grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.json

# run with specific credentials,settings and/or for all stat files in a folder
go run ./cmd/... -c ~/credentials -s ./settings.json -l ./load_spec.json -i ./test_data/
go run ./cmd/... -c ~/credentials -s ./settings.json -l ./load_spec.json -i /Users/gopa.padmanabhan/scratch/data/MET/G2G_v12/G2G_v12/20241104-06z/grid_stat
go run ./cmd/... -c ~/credentials -s ./settings.json -l ./load_spec.json -i /Users/gopa.padmanabhan/scratch/data/MET/
# recursive with file pattern regex match
go run ./cmd/... -c ~/credentials -s ./settings.json -l ./load_spec.json -I /Users/gopa.padmanabhan/scratch/data/MET/tc_data/tc_data/ -r ".tcst"
# if -f,-F,-i OR -I options are specified, ignores load_spec input files
```

## Output location, configuration and logic
Output will be in settings.json[OutputFolder], defaults to "./outputs", one file for each doc-id.
MET_cb_[docId].json.  It is important to note that if 
a file with same doc-id extis prior to run, the data from
current run will be merged with existing contents of that file.

On the Db side, the merge/overwrite logic is as below:

if settings.json (overWriteData == true)
    no merge is performed, current run will overwrite docs with same ids
if settings.json (overWriteData == false)
    merge is performed, current run will merge docs with same ids

A flush(merge) and/or Db merge is trigerred when a doc data section count reaches
settings.json setting: flushToDbDataSectionMaxCount


## Log output to file
By default, log is printed to stdout, but if you instead want to log to a file, add below at the end of above run commands:
2> logfile (for overwriting)
2>> logfile (for appending)

## Troubleshooting
If a troubleshoot.json file exists in the working folder, it would be used to log specific troubleshooting information.
For example, this file can be used to track extra logging for a document with a specific ID.
For more detailed information on troubleshooting, please refer to the troubleshooting.txt document.