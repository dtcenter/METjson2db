package utils

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/dtcenter/METjson2db/pkg/types"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("db-utils:init()")
}

func GetDbConnection(cred types.Credentials) (conn types.CbConnection) {
	slog.Debug(fmt.Sprintf("getDbConnection(%s.%s.%s)", cred.Cb_bucket, cred.Cb_scope, cred.Cb_collection))

	conn = types.CbConnection{}
	connectionString := cred.Cb_host
	bucketName := cred.Cb_bucket
	collection := cred.Cb_collection
	username := cred.Cb_user
	password := cred.Cb_password

	options := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	}

	cluster, err := gocb.Connect(connectionString, options)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		return conn
	}

	conn.Cluster = cluster
	conn.Bucket = conn.Cluster.Bucket(bucketName)
	conn.Collection = conn.Bucket.Collection(collection)

	conn.VxDBTARGET = cred.Cb_bucket + "." + cred.Cb_scope + "." + cred.Cb_collection
	// slog.Debug("vxDBTARGET:" + conn.vxDBTARGET)

	err = conn.Bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		return conn
	}

	conn.Scope = conn.Bucket.Scope(cred.Cb_scope)
	return conn
}

func QueryWithSQLFile(scope *gocb.Scope, file string) (jsonOut []string) {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
	}

	// Convert []byte to string
	text := string(fileContent)
	return QueryWithSQLStringSA(scope, text)
}

func QueryWithSQLStringSA(scope *gocb.Scope, text string) (rv []string) {
	slog.Debug("queryWithSQLStringSA(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprint(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
	}

	// Interfaces for handling streaming return values
	retValues := []string{}

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
		}
		retValues = append(retValues, row.(string))
	}

	return retValues
}

func QueryWithSQLStringFA(scope *gocb.Scope, text string) (rv []float64) {
	slog.Debug("queryWithSQLStringFA(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprint(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
	}

	retValues := make([]float64, 0)

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
		}
		retValues = append(retValues, row.(float64))
	}

	return retValues
}

func QueryWithSQLStringIA(scope *gocb.Scope, text string) (rv []int) {
	slog.Debug("queryWithSQLStringFA(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprint(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
	}

	retValues := make([]int, 0)

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
		}
		switch row := row.(type) {
		case float64:
			retValues = append(retValues, int(row))
		case int:
			retValues = append(retValues, row)
		}
	}

	return retValues
}

func GetDocWithId(col *gocb.Collection, id string) (jsonOut map[string]interface{}) {
	slog.Debug("getDocWithId(\n" + id + "\n)")

	queryResult, err := col.Get(id, nil)
	if err != nil {
		return nil
	}
	var doc map[string]interface{}
	err = queryResult.Content(&doc)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
	}
	return doc
}

func QueryWithSQLStringMAP(scope *gocb.Scope, text string) (jsonOut []interface{}) {
	slog.Debug("queryWithSQLStringMAP(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprint(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
	}

	rows := make([]interface{}, 0)

	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
		}
		m := row.(map[string]interface{})
		rows = append(rows, m)
	}
	return rows
}

func QueryWithSQLFileJustPrint(scope *gocb.Scope, file string) {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
	}

	// Convert []byte to string
	text := string(fileContent)

	queryResult, err := scope.Query(
		fmt.Sprint(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
	} else {
		PrintQueryResult(queryResult)
	}
}

func PrintQueryResult(queryResult *gocb.QueryResult) {
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
		} else {
			fmt.Println(result)
		}
	}
}
