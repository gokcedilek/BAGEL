package main

import (
	"fmt"
	"project/util"
)

func main() {
	err := util.SynchronizeConfigs()
	if err != nil {
		fmt.Println("Failed to synchronize config files", err)
	}
}
