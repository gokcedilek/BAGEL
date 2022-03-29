package main

import (
	"project/coord"
	"project/util"
)

func main() {
	var config coord.CoordConfig
	util.ReadJSONConfig("config/coord_config.json", &config)
}
