package main

import (
	"io"
	"log"
	"os"
	"project/database/mongodb"
)

const (
	PROD  = "prod"
	DEV   = "dev"
	SETUP = "setup"
	BAGEL = "bagel-test"
)

func main() {
	logFile, err := os.OpenFile(
		"bagel.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	log.SetPrefix("Database" + ": ")

	if len(os.Args) != 4 {
		log.Printf(
			"Usage: ./bin/database [$1 prod|dev] [$2 TABLE_NAME] [$3" +
				"<PATH_TO_GRAPH.txt>]",
		)
		return
	}

	//svc := database.GetDynamoClient()
	//if os.Args[1] == SETUP {
	//	database.CreateTableIfNotExists(svc, os.Args[2])
	//	database.AddGraph(
	//		svc, fmt.Sprintf("%s/%s", util.GetProjectRoot(), os.Args[3]),
	//		os.Args[2],
	//	)
	//}

	//mode := os.Args[1]
	tableName := os.Args[2]
	filepath := os.Args[3]

	if os.Args[1] == PROD {
		client := mongodb.GetDatabaseClient()

		mongodb.AddGraph(client, filepath, tableName)

		//collection := mongodb.GetCollection(client, tableName)
		//v, err := mongodb.GetVertexById(collection, 8)
		//log.Printf("vertex: %v, error: %v\n", v, err)
		//
		//vertices, err := mongodb.GetPartitionForWorkerX(collection, 3, 1)
		//log.Printf("partition vertices: %v\n", vertices)
	}

}
