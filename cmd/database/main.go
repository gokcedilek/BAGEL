package main

import (
	"io"
	"log"
	"os"
	"project/database"
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

	if len(os.Args) != 3 {
		log.Printf("Usage: ./bin/database [$1 TABLE_NAME] [$2<PATH_TO_GRAPH.txt>]")
		return
	}

	svc := database.GetDynamoClient()
	database.GetPartitionForWorkerX(svc, BAGEL, 2, 0)
}
