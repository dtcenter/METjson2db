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

statToCbDoc picks up Couchbase conection information from this file, example below
Please note that the cb_user and cb_password values should be replaced with actual values
cb_host: couchbase://adb-cb1.gsd.esrl.noaa.gov
cb_user: ***
cb_password: ***
cb_bucket: vxdata
cb_scope: _default
cb_collection: METAR

To pint to cluster, use
cb_host: adb-cb2.gsd.esrl.noaa.gov,adb-cb3.gsd.esrl.noaa.gov,adb-cb4.gsd.esrl.noaa.gov

# MySQL credentials file
NOTE: If no specific credentails file is given on the command-line, statToCbDoc
will look for and use ~/mysql.credentials

statToCbDoc picks up MySQL conection information from this file, example below:

role: sums_data,
status: active,
host: metv-gsd.gsd.esrl.noaa.gov,
port: 3306,
user: met_admin,
password: ***,
database: metexpress_metadata,
connectionLimit: 4



## updating the metadata
## cd METdatacb/statToCbDoc
go build .
## run using ~/credentials, ./settings.json for all apps
go run .
## run with specific credentials,settings and/or for a specific stat file
go run . -c ~/credentials -s ./settings.json -f /Users/gopa.padmanabhan/scratch/data/MET/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.stat

go run . -c ~/credentials -s ./settings.json -f ./test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_240000L_20240203_120000V.stat

Output will be in ./outputs with file name with extension as json, like:
grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.json

## run with specific credentials,settings and/or for all stat files in a folder
go run . -c ~/credentials -s ./settings.json -i /Users/gopa.padmanabhan/scratch/data/MET/

Output will be in ./outputs with file name with extension as json, like:
grid_stat_GFS_TMP_vs_ANLYS_TMP_Z2_420000L_20240203_120000V.json


