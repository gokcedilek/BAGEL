package main

import (
	"fmt"
	"project/bagel"
	"project/util"
	"sync"
	"time"
)

func main() {
	var config bagel.WorkerConfig
	numWorkers := 3

	workerWG := new(sync.WaitGroup)
	workerWG.Add(numWorkers)

	workerAddr := 43460

	// leaving this loop in for when we scale up number of workers
	for i := 1; i <= numWorkers; i++ {

		util.ReadJSONConfig(
			fmt.Sprintf("config/worker%v_config.json", i), &config,
		)
		fmt.Sprintf("worker id: %v\n", config.WorkerId)

		worker := bagel.NewWorker(config)

		go worker.Start()
		workerAddr++
		time.Sleep(2 * time.Second)
	}

	workerWG.Wait()
}
