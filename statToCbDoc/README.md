# METdatacb => statToCbDoc
# GO lang converter for MET stat files to Couchbase document JSON 

# Compile time requirement
Go runtime >= 1.21.3

# Runtime dependencies
Make sure settings.json is in the same folder as the statToCbDoc executable

### configuration
# Couchbase credentials file
NOTE: If no specific credentails file is given on the command-line, statToCbDoc
will look for and use ~/credentials
There is a sample credentials file at: METdatacb/statToCbDoc/credentials.template

statToCbDoc picks up Couchbase conection information from this file, example below
Please note that the cb_user and cb_password values should be replaced with actual values
cb_host: couchbase://adb-cb1.gsd.esrl.noaa.gov
cb_user: ***
cb_password: ***
cb_bucket: vxdata
cb_scope: _default
cb_collection: METAR

To point to cluster, use
cb_host: adb-cb2.gsd.esrl.noaa.gov,adb-cb3.gsd.esrl.noaa.gov,adb-cb4.gsd.esrl.noaa.gov


## cd METdatacb/statToCbDoc
go build .
## run using ~/credentials, ./settings.json , ./load_spec.json 
go run . -c ~/credentials -s ./settings.json -l ./load_spec.json
# to test for race conditions, add "-race" as below
go run -race . -c ~/credentials -s ./settings.json -l ./load_spec.json
## run with specific credentials,settings, load_spec and/or for a specific stat file
go run . -c ~/credentials -s ./settings.json -l ./load_spec.json -f /Users/gopa.padmanabhan/scratch/data/MET/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.stat
# if -f option is specified, ignores load_spec input files

go run . -c ~/credentials -s ./settings.json -l ./load_spec.json -f ./test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_240000L_20240203_120000V.stat

Output will be in ./outputs with file name with extension as json, like:
grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.json

## run with specific credentials,settings and/or for all stat files in a folder
go run . -c ~/credentials -s ./settings.json -l ./load_spec.json -i ./test_data/
go run . -c ~/credentials -s ./settings.json -l ./load_spec.json -i /Users/gopa.padmanabhan/scratch/data/MET/
# if -f option is specified, ignores load_spec input files

# Output location, configuration and logic
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


## log output to file
By default, log is printed to stdout, but if you instead want to log to a file, add below at the end of above run commands:
2> logfile (for overwriting)
2>> logfile (for appending)

