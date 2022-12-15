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
	PROD                 = "prod"
	DEV                  = "dev"
	SETUP                = "setup"
	BAGEL                = "bagel-test"
	SHRINK               = "shrink"
	SHRINK_NEW_FILE      = "COMPRESSED_GRAPH_FILENAME"
	SHRINK_DESIRED_NODES = "DESIRED_NODE_NUM"
	SHRINK_ORIGINAL_FILE = "ORIGINAL_GRAPH_FILENAME"
)

var SHRINK_ARG_MAP = map[string]int{
	SHRINK_NEW_FILE:      2,
	SHRINK_DESIRED_NODES: 3,
	SHRINK_ORIGINAL_FILE: 4,
}

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

	if len(os.Args) != 4 && len(os.Args) != 5 {
		fmt.Println(len(os.Args))
		printDatabaseUsages()
		return
	}

	svc := database.GetDynamoClient()
	if os.Args[1] == SETUP {
		database.CreateTableIfNotExists(svc, os.Args[2])
		database.AddGraph(
			svc, fmt.Sprintf("%s/%s", util.GetProjectRoot(), os.Args[3]),
			os.Args[2],
		)
	} else if os.Args[1] == SHRINK {
		graph := database.ParseInputGraph(getFileInRoot(os.Args[SHRINK_ARG_MAP[SHRINK_ORIGINAL_FILE]]), false)
		xNodes, err := strconv.ParseInt(os.Args[SHRINK_ARG_MAP[SHRINK_DESIRED_NODES]], 10, 32)
		if err != nil {
			panic(err)
		}
		shrunk := database.ReduceGraphToXNodes(graph, int(xNodes), 5)
		fmt.Printf("Original # Nodes %v Shrink # Nodes %v\n", len(graph), len(shrunk))
		err = database.WriteGraphToFile(os.Args[SHRINK_ARG_MAP[SHRINK_NEW_FILE]], shrunk)
		if err != nil {
			panic(err)
		}
	} else {
		log.Printf("Unrecognized command. See usage below\n")
		printDatabaseUsages()
	}
}

func getFileInRoot(file string) string {
	return fmt.Sprintf("%s/%s", util.GetProjectRoot(), file)
}

func printDatabaseUsages() {
	log.Printf("Usage: ./bin/database [setup|shrink] <options> (see below)")
	log.Printf("\tUsage: ./bin/database %s [$2 TABLE_NAME] [$3<PATH_TO_GRAPH.txt>]\n", SETUP)
	log.Printf("\tUsage: ./bin/database %s [$%d %s.txt] [$%d 3 %s] [$%d <%s.txt>]\n",
		SHRINK,
		SHRINK_ARG_MAP[SHRINK_NEW_FILE], SHRINK_NEW_FILE,
		SHRINK_ARG_MAP[SHRINK_DESIRED_NODES], SHRINK_DESIRED_NODES,
		SHRINK_ARG_MAP[SHRINK_ORIGINAL_FILE], SHRINK_ORIGINAL_FILE,
	)
}
