package database

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash/fnv"
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

type Vertex struct {
	vertexID     uint64
	vertexIDHash uint64
	neighbors    []uint64
}

type DatabaseConfig struct {
	DatabaseName string
	ServerAddr   string
	Port         int
	Username     string
	Password     string
	Database     string
	TableName    string
}

var (
	nodeAdjacencyList map[uint32][]uint32
	db                *sql.DB
	dbName            = "bagelDB_new"
	tableName         = "adjList"
	server            = "bagel.database.windows.net"
	port              = 1433
	user              = "user"
	password          = "Distributedgraph!"
	database          = "bagel_2.0"
)

// func main() {
// 	//initializeDB(dbName)

// 	connectToDb()
// 	// createAdjList()
// 	// addAdjList()

// 	v, err := getVertex(728666)
// 	if err != nil {
// 		panic("shit hit the fan")
// 	}
// 	fmt.Printf("Vertex found: %v\n", v)
// }

func SetupDatabase(config DatabaseConfig) error {
	initializeDB(config.DatabaseName)
	err := connectToDb()
	createAdjList()
	addAdjList()

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
	fmt.Println("connected to DB successfully!")
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
	//_, err = db.Exec("CREATE DATABASE " + name)
	if err != nil {
		panic(err)
	}

	//_, err = db.Exec("USE " + name)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("DROP TABLE " + tableName)
	_, err = db.Exec("CREATE TABLE " + tableName + " (srcVertex integer, hash VARCHAR(8000), neighbors VARCHAR(8000))")
	if err != nil {
		panic(err)
	}
}

// `nohup go run db-test.go &`
func addAdjList() {
	if db == nil {
		fmt.Println("Not connected to Database yet")
		panic("aaa")
	}
	if nodeAdjacencyList == nil {
		fmt.Println("Adj list is empty")
		panic("aaa")
	}
	for id, neighbors := range nodeAdjacencyList {
		hash := HashId(uint64(id))
		neighborsString := arrayToString(neighbors, ".") //normal delimiters cause problems with SQL
		dbQueryString := "INSERT INTO " + tableName + " (srcVertex, hash, neighbors) VALUES (" + strconv.Itoa(int(id)) + ", '" + strconv.FormatUint(hash, 10) + "', '" + neighborsString + "')"
		fmt.Println(dbQueryString)
		_, err := db.Exec(dbQueryString)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Inserted %v and its %v neighbors\n", id, len(neighbors))
	}
}

func getVertex(id int) (*Vertex, error) {
	if db == nil {
		fmt.Println("Not connected to Database yet")
		panic("aaa")
	}
	rows, err := db.Query("SELECT * FROM " + tableName + " WHERE srcVertex = " + strconv.Itoa(id) + ";")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Query for a value based on a single row.
	var searchID uint64
	var hash string
	var neighbors string
	qs := "SELECT * FROM " + tableName + " WHERE srcVertex = " + strconv.Itoa(id) + ";"
	if err := db.QueryRow(qs).Scan(&searchID, &hash, &neighbors); err != nil {
		if err == sql.ErrNoRows {
			return &Vertex{}, fmt.Errorf("%d: unknown ID", id)
		}
		return &Vertex{}, fmt.Errorf("some kind of error :| %d", id)
	}
	hashNum, err := strconv.ParseUint(hash, 10, 64)
	if err != nil {
		panic("parsing hash to Uint64 failed")
	}
	v := Vertex{vertexID: searchID, vertexIDHash: hashNum, neighbors: stringToArray(neighbors, ".")}
	return &v, nil
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

func HashId(vertexId uint64) uint64 {
	inputBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(inputBytes, vertexId)

	algorithm := fnv.New64a()
	algorithm.Write(inputBytes)
	return algorithm.Sum64()
}

// https://stackoverflow.com/questions/37532255/one-liner-to-transform-int-into-string
func arrayToString(a []uint32, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}

func stringToArray(a string, delim string) []uint64 {
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

func createAdjList() {
	nodeAdjacencyList = make(map[uint32][]uint32)
	var graphLocation string = "./web-Google.txt"
	file, err := os.Open(graphLocation)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// n := Node{}
	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "#") {
			continue
		}
		edge := strings.Split(line, "\t")
		vertex_0, _ := strconv.ParseUint(edge[0], 10, 32)
		vertex_1, _ := strconv.ParseUint(edge[1], 10, 32)
		nodeAdjacencyList[uint32(vertex_0)] = append(nodeAdjacencyList[uint32(vertex_0)], uint32(vertex_1))
		if nodeAdjacencyList[uint32(vertex_1)] == nil {
			nodeAdjacencyList[uint32(vertex_1)] = []uint32{}
		}
		lineNum = lineNum + 1
	}
	fmt.Printf("finished! built map of size %v\n", len(nodeAdjacencyList))
	fmt.Printf("iterated over %v lines\n", lineNum)
	largest_neighbors := 0
	largest_neighbors_node := 0
	for key, element := range nodeAdjacencyList {
		if len(element) > largest_neighbors {
			largest_neighbors = len(element)
			largest_neighbors_node = int(key)
		}
	}
	fmt.Printf("Node with the most neighbors is %v with %v neighbors\n", largest_neighbors_node, largest_neighbors)
}
