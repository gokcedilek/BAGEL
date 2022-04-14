package main

import (
	"io"
	"log"
	"os"
	"project/bagel"
	"project/util"
)

func main() {

	// create a log file and log to both console and terminal
	logFile, err := os.OpenFile("bagel.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	log.SetPrefix("Coord: ")

	var config bagel.CoordConfig
	util.ReadJSONConfig("config/coord_config.json", &config)
	util.CheckErr(err, "Error reading coord config: %v\n", err)

	coord := bagel.NewCoord()
	coord.Start(config.ClientAPIListenAddr, config.WorkerAPIListenAddr, config.LostMsgsThresh, config.StepsBetweenCheckpoints)
}
