package main

import (
	"fmt"
	"os"
	"project/util"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: ./bin/cnf [sync|port|azure|update]")
		fmt.Println("example ./bin/cnf sync")
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
	} else if os.Args[1] == "azure" && len(os.Args) == 4 {
		err := util.AssignToRemote(os.Args[2], os.Args[3])
		if err != nil {
			fmt.Println("Failed to assign remote addresses - ", err)
		}
	} else {
		fmt.Println("usage: ./bin/cnf [sync|port|azure] [coordRemoteServer] [clientRemoteServer]")
		fmt.Println("example ./bin/cnf sync")
		fmt.Println("example ./bin/cnf azure Lulu Anvil")
	}
	//else if os.Args[1] == "update" && len(os.Args) == 3 && os.
	//	Args[2] == "docker" {
	//	err := util.UpdateToDockerAddrs();
	//} else if os.Args[1] == "update" && len(os.Args) == 3 && os.
	//	Args[2] == "local" {
	//
	//}

}
