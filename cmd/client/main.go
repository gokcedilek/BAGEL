package main

import (
	"io"
	"log"
	"os"
	"project/bagel"
	"project/util"
	"strconv"
	"strings"
)

func main() {
	// read config
	var config bagel.ClientConfig
	err := util.ReadJSONConfig("config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	// create a log file and log to both console and terminal
	logFile, err := os.OpenFile(
		"bagel.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	// logs all start with ClientId from config
	log.SetPrefix(config.ClientId + ": ")

	log.Printf("Client: main.go: args: %v\n", os.Args)

	invalidInput := false
	var query bagel.Query

	if len(os.Args) < 3 || len(os.Args) > 4 {
		invalidInput = true
	} else if strings.EqualFold(os.Args[1], bagel.PAGE_RANK) {
		if len(os.Args) != 3 {
			invalidInput = true
		} else {
			v1, err := strconv.Atoi(os.Args[2])
			if err != nil {
				log.Println("Provided vertex could not be converted to integer")
				invalidInput = true
			} else {
				query.QueryType = bagel.PAGE_RANK
				query.Nodes = []uint64{uint64(v1)}
			}
		}
	} else if strings.EqualFold(os.Args[1], bagel.SHORTEST_PATH) {
		if len(os.Args) != 4 {
			invalidInput = true
		} else {
			v1, err := strconv.Atoi(os.Args[2])
			if err != nil {
				log.Println("Provided vertex could not be converted to integer")
				invalidInput = true
			}
			v2, err := strconv.Atoi(os.Args[3])
			if err != nil {
				log.Println("Provided vertex could not be converted to integer")
				invalidInput = true
			} else {
				query.QueryType = bagel.SHORTEST_PATH
				query.Nodes = []uint64{uint64(v1), uint64(v2)}
			}
		}
	} else {
		invalidInput = true
	}

	if invalidInput {
		log.Println("Usage: ./bin/client [shortestpath|pagerank] [vertexId] [vertexId]")
		log.Println("Example: ./bin/client pagerank 11")
		log.Println("Example: ./bin/client shortestpath 11 54")
		return
	}

	client := bagel.NewClient()
	notifyCh, err := client.Start(
		config.ClientId, config.CoordAddr, config.ClientAddr,
	)
	util.CheckErr(err, "Error connecting to coord: %v\n", err)

	numQueries := 1
	err = client.SendQuery(query)
	util.CheckErr(err, "Error sending query: %v\n", err)
	log.Printf("Client sent %v with %v\n", query.QueryType, query.Nodes)

	for i := 0; i < numQueries; i++ {
		result := <-notifyCh
		if result.Error != "" {
			log.Printf("Client: SendQuery error: %v\n", result.Error)
		}
		log.Printf("Client: SendQuery received result: %v\n", result.Result)
	}

}
