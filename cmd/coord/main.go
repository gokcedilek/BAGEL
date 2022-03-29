package main

import (
	"project/bagel"
	"project/util"
)

func main() {
	var config bagel.CoordConfig
	util.ReadJSONConfig("config/coord_config.json", &config)

	coord := bagel.NewCoord()
	coord.Start(config.ClientAPIListenAddr, config.WorkerAPIListenAddr, config.LostMsgsThresh, config.StepsBetweenCheckpoints)
}
