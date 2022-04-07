package main

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

type Node struct {
	nodeID    uint32
	neighbors []uint32
}

var db *sql.DB
var dbName = "bagelDB"
var tableName = "graph"
var server = "bagel.database.windows.net"
var port = 1433
var user = "user"
var password = "Distributedgraph!"
var database = "Graph_Backup_DB"

func main() {
	start := time.Now()
	n, err := GetNode(0)
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("Found node: %v in %s\n", n, elapsed)
}

func connectToDb() (*sql.DB, error) {
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

func GetNode(id int) (*Node, error) {
	_, err := connectToDb()
	if err != nil {
		panic(err)
	}
	if db == nil {
		fmt.Println("Not connected to Database yet")
		panic("aaa")
	}

	rows, err := db.Query("SELECT * FROM " + tableName + " WHERE vertex1 = " + strconv.Itoa(id) + ";")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// An album slice to hold data from returned rows.
	var neighbors []uint32

	// Loop through rows, using Scan to assign column data to struct fields.
	var currId uint32
	for rows.Next() {
		var neighbor uint32
		if err := rows.Scan(&currId, &neighbor); err != nil {
			n := Node{nodeID: currId, neighbors: neighbors}
			return &n, err
		}
		neighbors = append(neighbors, neighbor)
	}
	n := Node{nodeID: currId, neighbors: neighbors}
	if err = rows.Err(); err != nil {

		return &n, err
	}
	return &n, nil
}
