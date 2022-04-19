package database

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"project/util"
	"strconv"
	"strings"
	"time"

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

// InitializeDBVars Way of overwriting the db vars used in both db-setup and db-functions
// 	Better to init as struct, but this will require both clients and coord to be aware
func InitializeDBVars(config DatabaseConfig) {
	dbName = config.DatabaseName
	tableName = config.TableName
	server = config.ServerAddr
	port = config.Port
	user = config.Username
	password = config.Password
	database = config.Database
}

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

func SetupDatabase(config DatabaseConfig) {
	InitializeDBVars(config)
	initializeDB()
	err := connectToDb()

	if err != nil {
		log.Fatal("Could not establish connection to database. Exiting")
	}
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

func initializeDB() {
	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)
	var err error
	// Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}

	_, err = db.Exec("DROP TABLE " + tableName)
	_, err = db.Exec("CREATE TABLE " + tableName +
		" (srcVertex integer, hash BIGINT, neighbors VARCHAR(8000))")
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

	err := BulkInsert(nodeAdjacencyList, 3, "srcVertex, hash, neighbors")
	if err != nil {
		log.Fatal("Failed to insert nodes", err)
	}
}

// https://stackoverflow.com/questions/37532255/one-liner-to-transform-int-into-string
func arrayToString(a []uint32, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
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

func BulkInsert(unsavedRows map[uint32][]uint32, numParams int, params string) error {
	const maxParamsSQL = 2099
	rowsPerInsert := maxParamsSQL / numParams
	N := int(math.Ceil(float64(len(unsavedRows)) / float64(rowsPerInsert)))

	bulks := getBulks(unsavedRows, N, rowsPerInsert)

	for i := 0; i < N; i++ {
		startTime := time.Now()
		valueStrings := make([]string, 0, rowsPerInsert)
		valueArgs := make([]interface{}, 0, rowsPerInsert*numParams)

		startOrdinalPosition := 1
		for id, neighbors := range bulks[i] {
			neighborsString := arrayToString(neighbors, ".") //normal delimiters cause problems with SQL
			valueStrings = append(valueStrings, GetParamPlaceHolders(startOrdinalPosition))
			valueArgs = append(valueArgs, id)
			valueArgs = append(valueArgs, util.HashId(uint64(id)))
			valueArgs = append(valueArgs, neighborsString)
			startOrdinalPosition += numParams
		}
		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;",
			tableName, params, strings.Join(valueStrings, ","))
		_, err := db.Exec(stmt, valueArgs...)
		if err != nil {
			log.Fatal("Failed to bulk insert rows", err)
			return err
		}
		log.Printf("Successfully inserted (%d/%d) time elapsed %v\n", i, N, time.Since(startTime))
	}
	return nil
}

func GetParamPlaceHolders(startOrdinalPosition int) string {
	return fmt.Sprintf("(@p%d, @p%d, @p%d)",
		startOrdinalPosition,
		startOrdinalPosition+1,
		startOrdinalPosition+2)
}

func getBulks(total map[uint32][]uint32, n int, rowsPerBulk int) []map[uint32][]uint32 {
	bulks := make([]map[uint32][]uint32, 0, n)

	for i := 0; i < n; i++ {
		bulks = append(bulks, make(map[uint32][]uint32))
	}

	cnt := 0

	for id, neighbors := range total {
		idx := cnt / rowsPerBulk
		bulks[idx][id] = neighbors
		cnt++
	}

	return bulks
}
