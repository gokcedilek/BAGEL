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

	workerWG := new(sync.WaitGroup)
	workerWG.Add(1)

	workerAddr := 43460

	// leaving this loop in for when we scale up number of workers
	for i := 1; i < 2; i++ {

		util.ReadJSONConfig(
			fmt.Sprintf("config/worker%v_config.json", i), &config,
		)
		fmt.Sprintf("worker id: %v\n", config.WorkerId)

		worker := bagel.NewWorker()

		go worker.Start(
			config.WorkerId, config.CoordAddr,
			fmt.Sprintf("127.0.0.1:%v", workerAddr) /*config.workerAddr*/, config.WorkerListenAddr)
		workerAddr++
		time.Sleep(2 * time.Second)
	}

	workerWG.Wait()
}
