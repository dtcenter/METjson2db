# METdatacb

# MET stat file to Couchbase JSON document converter and uploader
This project is a GO language command-line program to generate Couchbase JSON documents 
and/or
upload these documents to Couchbase database.
 

## Inline upload vs JSON archive generation
There are two ways this program can be run. The run mode is specified in the settings.json file.
Refer to documentation for a detailed description of settings.json 
The 2 run modes are: 
"runMode" : "DIRECT_LOAD_TO_DB"
OR
"runMode" : "CREATE_JSON_DOC_ARCHIVE"
Please note that the setting:
"overWriteData": false
which enables merge functionality, wherein incoming data is merged with data already in the database
is only available in run mode:
"runMode" : "DIRECT_LOAD_TO_DB"


## The 2-step process
### STEP 1 : Generate line type definitions for a particular MET version
This step needs to be done once for each MET version, and the genrerated code checked in to MET-parser git repo:main
Refer to documentation section "Generating LINE_TYPE definitions" for details on how to do this
### STEP 2 : Run METdadacb to generate and/or upload JSON documents to Couchbase/Capella

# Couchbase
Unlike relational databases like MySQL, PostGres etc, Couchbase is a JSON document based database where
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
See METdatacb/credentials.template for the content and syntax of this creadentials file.
The Collection itself can be overridden in the load_spec.json file, thus allowing for data to be stored in separate collections
for research and comparison purposes.

### Queries and indexes
Couchbase queries are similar to SQL with some additional sematics for dealing with JSON data,and is called SQL++
See this link for SQL++ documentation:
https://docs.couchbase.com/server/current/n1ql/query.html 
Indexes serve similar purpose that in relational databases, namely, to speed up specific queries.
SQL++ statements for creating the required indexes are in : METdatacb/indexes
See see section installing and configuring Couchbase for more information on creating indexes


## MET Couchbase Document format (data model)
Describe in brief about header & data sections, data key used


# Installing and configuring Couchbase for METdatacb
## Create Bucket, Scope and Collection(s)
## Create required indexes
## Couchbase index adviser






# Generating LINE_TYPE definitions
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

## Configuration "settings.json"
TODO - document settings.json 


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


## Log output to file
By default, log is printed to stdout, but if you instead want to log to a file, add below at the end of above run commands:
2> logfile (for overwriting)
2>> logfile (for appending)

