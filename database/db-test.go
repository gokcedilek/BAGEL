package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

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
	err := connectToDb()
	if err != nil {
		panic(err)
	}
	addGraph()
	if err != nil {
		panic(err)
	}
	// fmt.Printf("Node found: %v", n)
}

func connectToDb() error {
	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)
	var err error
	// Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
		return err
	}
	return nil
}

func initializeDB(name string) {
	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)
	var err error
	// Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	// _,err = db.Exec("CREATE DATABASE "+name)
	if err != nil {
		panic(err)
	}

	// _,err = db.Exec("USE "+name)
	if err != nil {
		panic(err)
	}

	// _,err = db.Exec("CREATE TABLE graph (vertex1 integer, vertex2 integer )")
	if err != nil {
		panic(err)
	}

}

func addGraphOld(name string) {
	// fmt.Printf("%v", db)
	if db == nil {
		fmt.Println("Not connected to Database yet")
		panic("aaa")
	}

	_, err := db.Exec("INSERT INTO " + tableName + " (vertex1, vertex2) VALUES (-1, -1)")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("DELETE FROM " + tableName + " WHERE vertex1=-1;")
	if err != nil {
		panic(err)
	}
}

func getNode(id int) (*Node, error) {
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

func addGraph() {
	var graphLocation string = "./web-Google.txt"
	if db == nil {
		fmt.Println("Not connected to Database yet")
		panic("aaa")
	}
	file, err := os.Open(graphLocation)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// n := Node{}
	scanner := bufio.NewScanner(file)
	lineNum := 0
	insertionTime := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "#") {
			continue
		}
		edge := strings.Split(line, "\t")
		vertex_0, _ := strconv.ParseUint(edge[0], 10, 32)
		vertex_1, _ := strconv.ParseUint(edge[1], 10, 32)
		if vertex_0 == 449461 && vertex_1 == 454013 {
			fmt.Printf("Im on line %v\n", lineNum)
			insertionTime = true
			panic("aaaa")
		}
		lineNum = lineNum + 1
		if insertionTime {
			fmt.Printf("Inserting %v, %v\n", vertex_0, vertex_1)
			_, err := db.Exec("INSERT INTO " + tableName + " (vertex1, vertex2) VALUES (" + strconv.Itoa(int(vertex_0)) + ", " + strconv.Itoa(int(vertex_1)) + ")")
			if err != nil {
				panic(err)
			}
		}
	}
}
