package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"project/database"
	"project/util"
	"strconv"
)

const (
	PROD   = "prod"
	DEV    = "dev"
	SETUP  = "setup"
	BAGEL  = "bagel-test"
	SHRINK = "shrink"
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

	if len(os.Args) != 4 || len(os.Args) != 6 {
		log.Printf("Usage: ./bin/database [setup|shrink] <options> (see below)")
		log.Printf("\tUsage: ./bin/database %s [$1 TABLE_NAME] [$2<PATH_TO_GRAPH.txt>]\n", SETUP)
		log.Printf("\tUsage: ./bin/database %s [$1 NEW_FILE_NAME.txt] [$2 NUM_NODES_DESIRED] "+
			"[$3<PATH_TO_GRAPH.txt>]\n", SHRINK)
		return
	}

	svc := database.GetDynamoClient()
	if os.Args[1] == SETUP {
		database.CreateTableIfNotExists(svc, os.Args[2])
		database.AddGraph(
			svc, fmt.Sprintf("%s\\testGraph.txt", util.GetProjectRoot()),
			os.Args[2],
		)
		//database.AddGraph(
		//	svc, "./testGraph.txt", os.Args[2],
		//)
	} else if os.Args[1] == SHRINK {
		graph := database.ParseInputGraph(os.Args[4])
		xNodes, err := strconv.ParseInt(os.Args[3], 10, 32)
		if err != nil {
			panic(err)
		}
		shrunk := database.ReduceGraphToXNodes(graph, int(xNodes))
		err = database.WriteGraphToFile(os.Args[2], shrunk)
		if err != nil {
			panic(err)
		}
	}

}
