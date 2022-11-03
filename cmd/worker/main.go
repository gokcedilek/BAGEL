package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"project/bagel"
	"project/util"
	"strconv"
	"sync"
)

func main() {

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

	var config bagel.WorkerConfig

	workerWG := new(sync.WaitGroup)
	workerWG.Add(1)

	invalidInput := false
	workerId := 0

	if len(os.Args) != 2 {
		invalidInput = true
	} else {
		id, err := strconv.Atoi(os.Args[1])
		if err != nil {
			log.Println("Provided worker id could not be converted to integer")
			invalidInput = true
		} else {
			workerId = id
			// logs all start with WorkerId from config
			log.SetPrefix("Worker " + os.Args[1] + ": ")
		}
	}

	if invalidInput {
		log.Println("Usage: ./bin/worker [workerId]")
		return
	}

	util.ReadJSONConfig(
		fmt.Sprintf("config/worker%v_config.json", workerId), &config,
	)
	log.Printf("Main: starting Worker %v\n", config.WorkerId)

	log.Printf("config: %v\n", config)

	worker := bagel.NewWorker(config)

	go worker.Start()
	workerWG.Wait()
}
