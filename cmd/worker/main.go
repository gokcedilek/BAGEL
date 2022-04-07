package main

import (
	"fmt"
	"log"
	"os"
	"project/bagel"
	"project/util"
	"strconv"
	"sync"
)

func main() {
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
		}
	}

	if invalidInput {
		log.Println("Usage: ./bin/worker [workerId]")
		return
	}

	util.ReadJSONConfig(
		fmt.Sprintf("config/worker%v_config.json", workerId), &config,
	)
	fmt.Sprintf("worker id: %v\n", config.WorkerId)

	worker := bagel.NewWorker(config)

	go worker.Start()
	workerWG.Wait()
}
