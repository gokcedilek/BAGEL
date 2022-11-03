package database

/*

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"log"
	"strconv"
	"strings"
)

type DBVertexResult struct {
	VertexID     uint64
	vertexIDHash int64
	Neighbors    []uint64
}

func getDBConnection() (*sql.DB, error) {
	// Build connection string

	if db != nil {
		return db, nil
	}

	connString := fmt.Sprintf(
		"server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database,
	)
	var err error
	// Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
		return nil, err
	}

	log.Printf("Successfully connected to %s database!\n", dbName)
	return db, nil
}

func GetVertexById(id int) (*DBVertexResult, error) {
	//if db == nil {
	//	log.Println("Creating new connection to database")
	//	db, _ = getDBConnection()
	//}
	//
	//// Query for a value based on a single row.
	//var searchID int64
	//var hash int64
	//var neighbors string
	//qs := "SELECT * FROM " + tableName + " WHERE srcVertex = " + strconv.Itoa(id) + ";"
	//if err := db.QueryRow(qs).Scan(&searchID, &hash, &neighbors); err != nil {
	//	if err == sql.ErrNoRows {
	//		return &DBVertexResult{}, fmt.Errorf("%d: unknown ID", id)
	//	}
	//	return &DBVertexResult{}, fmt.Errorf(
	//		"Failed to retrieve vertex #%d %v\n", id, err,
	//	)
	//}
	//v := DBVertexResult{
	//	VertexID: uint64(searchID), vertexIDHash: hash,
	//	Neighbors: convertStringToArray(neighbors, "."),
	//}
	//return &v, nil
	var pow = []uint64{1, 2, 4, 8, 16, 32, 64, 128}

	for i, val := range pow {
		if val == uint64(id) {
			pianoVertex := DBVertexResult{
				VertexID:     uint64(id),
				vertexIDHash: int64(i),
				Neighbors:    pow,
			}
			return &pianoVertex, nil
		}
	}
	return nil, errors.New("error finding vertex")
}

func GetVerticesModulo(workerId uint32, numWorkers uint8) (
	[]DBVertexResult, error,
) {
	//if db == nil {
	//	db, _ = getDBConnection()
	//	fmt.Println("Creating new connection to database..")
	//}
	//
	//numWorkersString := strconv.Itoa(int(numWorkers))
	//whereClause := GetFlooredModuloString("hash", numWorkersString)
	//
	//queryString := fmt.Sprintf(
	//	"SELECT * FROM %s WHERE %s = %d;",
	//	tableName, whereClause, int(workerId))
	//
	//fmt.Println(queryString)
	//
	//result, err := db.Query(queryString)
	//
	//if err != nil {
	//	log.Fatal("GetVerticesModulo - failed to retrieve partition!", err)
	//}

	//var searchID int64
	//var hash int64
	//var neighbors string
	var vertices = []DBVertexResult{}
	var pow = []uint64{1, 2, 4, 8, 16, 32, 64, 128}
	//for result.Next() {
	//	err := result.Scan(&searchID, &hash, &neighbors)
	//	if err != nil {
	//		panic("scan went wrong")
	//	}
	//	v := DBVertexResult{VertexID: uint64(searchID), vertexIDHash: hash, Neighbors: convertStringToArray(neighbors, ".")}
	//	vertices = append(vertices, v)
	//}
	for i, val := range pow {
		v := DBVertexResult{
			VertexID: val, vertexIDHash: int64(i),
			Neighbors: pow,
		}
		vertices = append(vertices, v)
	}

	return vertices, nil
}

func convertStringToArray(a string, delim string) []uint64 {
	neighbors := strings.Split(a, delim)
	neighborSlice := []uint64{}
	if len(strings.TrimSpace(a)) == 0 {
		return neighborSlice
	}
	for _, v := range neighbors {
		neighborID, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			panic("parsing hash to Uint64 failed")
		}
		neighborSlice = append(neighborSlice, neighborID)
	}
	return neighborSlice
}

func GetFlooredModuloString(a string, b string) string {
	// returns string for DB where modulo result ranges in 0,... b
	// https://stackoverflow.com/questions/1907565/c-and-python-different-behaviour-of-the-modulo-operation
	modulo_op := "%"
	return fmt.Sprintf(
		"((%s %s %s) + %s) %s %s", a, modulo_op, b, b, modulo_op, b,
	)
}
*/
