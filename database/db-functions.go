package database

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

type DBVertexResult struct {
	VertexID     uint64
	vertexIDHash uint64
	Neighbors    []uint64
}

func main() {
	start := time.Now()
	n, err := GetVerticesModulo(1, 3)
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("Found vertex: %v in %s\n", n, elapsed)
}

func getDBConnection() (*sql.DB, error) {
	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)
	var err error
	// Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
		return nil, err
	}
	return db, nil
}

func GetVertexById(id int) (*DBVertexResult, error) {
	getDBConnection()
	if db == nil {
		fmt.Println("Not connected to Database yet")
		panic("aaa")
	}

	// Query for a value based on a single row.
	var searchID uint64
	var hash string
	var neighbors string
	qs := "SELECT * FROM " + tableName + " WHERE srcVertex = " + strconv.Itoa(id) + ";"
	if err := db.QueryRow(qs).Scan(&searchID, &hash, &neighbors); err != nil {
		if err == sql.ErrNoRows {
			return &DBVertexResult{}, fmt.Errorf("%d: unknown ID", id)
		}
		return &DBVertexResult{}, fmt.Errorf("some kind of error :| %d", id)
	}
	hashNum, err := strconv.ParseUint(hash, 10, 64)
	if err != nil {
		panic("parsing hash to Uint64 failed")
	}
	v := DBVertexResult{VertexID: searchID, vertexIDHash: hashNum, Neighbors: convertStringToArray(neighbors, ".")}
	return &v, nil
}

func GetVerticesModulo(workerId uint32, numWorkers uint8) ([]DBVertexResult, error) {
	getDBConnection()
	if db == nil {
		fmt.Println("Not connected to Database yet")
		panic("aaa")
	}
	result, err := db.Query(
		"SELECT * from " + tableName + " where srcVertex % " + strconv.Itoa(int(numWorkers)) + " = " + strconv.Itoa(int(workerId)) + ";")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		panic("query went wrong")
	}
	var searchID uint64
	var hash string
	var neighbors string
	var vertices = []DBVertexResult{}
	for result.Next() {
		err := result.Scan(&searchID, &hash, &neighbors)
		if err != nil {
			panic("scan went wrong")
		}
		hashNum, err := strconv.ParseUint(hash, 10, 64)
		if err != nil {
			panic("parsing hash to Uint64 failed")
		}
		v := DBVertexResult{VertexID: searchID, vertexIDHash: hashNum, Neighbors: convertStringToArray(neighbors, ".")}
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
