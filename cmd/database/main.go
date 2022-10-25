package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"project/database"
	"project/util"
)

const (
	PROD  = "prod"
	DEV   = "dev"
	SETUP = "setup"
	BAGEL = "bagel-test"
)

func main() {
	logFile, err := os.OpenFile("bagel.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	log.SetPrefix("Database" + ": ")

	if len(os.Args) != 4 {
		log.Printf("Usage: ./bin/database [$1 TABLE_NAME] [$2<PATH_TO_GRAPH.txt>]")
		return
	}

	svc := database.GetDynamoClient()
	if os.Args[1] == SETUP {
		database.CreateTableIfNotExists(svc, os.Args[2])
		database.AddGraph(svc, fmt.Sprintf("%s\\testGraph.txt", util.GetProjectRoot()), os.Args[2])
	}

}
