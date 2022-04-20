package main

import (
	"fmt"
	"os"
	"project/util"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: ./bin/config [sync|port]")
		fmt.Println("example ./bin/config sync")
		return
	}

	if os.Args[1] == "sync" {
		err := util.SynchronizeConfigs()
		if err != nil {
			fmt.Println("Failed to synchronize config files", err)
		}
	} else if os.Args[1] == "port" {
		err := util.AssignPorts()
		if err != nil {
			fmt.Println("Failed to assign port numbers to workers", err)
		}
	} else {
		fmt.Println("usage: ./bin/config [sync|port]")
		fmt.Println("example ./bin/config sync")
	}

}
