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
	numWorkers := 2

	workerWG := new(sync.WaitGroup)
	workerWG.Add(numWorkers)

	workerAddr := 43460

	// leaving this loop in for when we scale up number of workers
<<<<<<< HEAD
	for i := 1; i <= numWorkers; i++ {
=======
	for i := 1; i < 2; i++ {
>>>>>>> bc8c6b25bece03cfd5d3b74e8bcd12026ff90245

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
