package util

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

/*
	Config Files re-stated here to avoid circular dependency
    	project/bagel imports project/util
		(IF IMPORT) then project/util imports project/bagel
*/

type CoordConfig struct {
	ClientAPIListenAddr     string // client will know this and use it to contact coord
	WorkerAPIListenAddr     string // new joining workers will message this addr
	LostMsgsThresh          uint8  // fcheck
	StepsBetweenCheckpoints uint64
}

type WorkerConfig struct {
	WorkerId              uint32
	CoordAddr             string
	WorkerAddr            string
	WorkerListenAddr      string
	FCheckAckLocalAddress string
}

type ClientConfig struct {
	ClientId   string
	CoordAddr  string
	ClientAddr string
}

const (
	WORKERS    = "worker"
	CLIENT     = "client"
	COORD      = "coord"
	PORT_START = 49152
	PORT_END   = 65535
)

func SynchronizeConfigs() error {
	files, err := os.ReadDir("config")
	if err != nil {
		return err
	}

	var coord CoordConfig
	err = ReadJSONConfig(getConfigPath("coord_config.json"), &coord)
	if err != nil {
		return err
	}

	for _, file := range files {
		filename := file.Name()

		if isConfigType(filename, CLIENT) {
			var client ClientConfig
			err = ReadJSONConfig(getConfigPath(filename), &client)
			client.CoordAddr = coord.ClientAPIListenAddr
			err := WriteJSONConfig(getConfigPath(filename), client)
			if err != nil {
				return err
			}
		}
		if isConfigType(filename, WORKERS) {
			var worker WorkerConfig
			err = ReadJSONConfig(getConfigPath(filename), &worker)
			worker.CoordAddr = coord.WorkerAPIListenAddr
			err := WriteJSONConfig(getConfigPath(filename), worker)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func AssignPorts() error {
	portsAssigned := make(map[int]bool)

	files, err := os.ReadDir("config")
	if err != nil {
		return err
	}
	for _, file := range files {
		filename := file.Name()

		if isConfigType(filename, COORD) {
			err = reassignCoordPorts(filename, &portsAssigned)
		}

		if isConfigType(filename, CLIENT) {
			err = reassignClientPorts(filename, &portsAssigned)
		}

		if isConfigType(filename, WORKERS) {
			err = reassignWorkerPorts(filename, &portsAssigned)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func SetPort(ipPort string, port int) string {
	ip_port := strings.Split(ipPort, ":")
	ip_port[1] = strconv.Itoa(port)
	return strings.Join(ip_port, ":")
}

func isConfigType(filename string, configType string) bool {
	return strings.HasPrefix(filename, configType)
}

func getConfigPath(filename string) string {
	return fmt.Sprintf("config/%s", filename)
}

func getUnassignedPortNumber(assignedPorts map[int]bool) int {
	for {
		port := getRandomPortNumber()
		if _, exists := assignedPorts[port]; !exists {
			assignedPorts[port] = true
			fmt.Println("found port = ", port, assignedPorts)
			return port
		}
	}
}

func getRandomPortNumber() int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(PORT_END-PORT_START) + PORT_START
}

func reassignCoordPorts(filename string, assignedPorts *map[int]bool) error {
	var coord CoordConfig
	err := ReadJSONConfig(getConfigPath(filename), &coord)

	if err != nil {
		return err
	}

	port := getUnassignedPortNumber(*assignedPorts)
	coord.ClientAPIListenAddr = SetPort(coord.ClientAPIListenAddr, port)
	port = getUnassignedPortNumber(*assignedPorts)
	coord.WorkerAPIListenAddr = SetPort(coord.WorkerAPIListenAddr, port)

	err = WriteJSONConfig(getConfigPath(filename), coord)
	if err != nil {
		return err
	}
	return SynchronizeConfigs()
}

func reassignClientPorts(filename string, assignedPorts *map[int]bool) error {
	var client ClientConfig
	err := ReadJSONConfig(getConfigPath(filename), &client)

	if err != nil {
		return err
	}

	port := getUnassignedPortNumber(*assignedPorts)
	client.ClientAddr = SetPort(client.ClientAddr, port)
	return WriteJSONConfig(getConfigPath(filename), client)
}

func reassignWorkerPorts(filename string, assignedPorts *map[int]bool) error {

	fmt.Printf("Reassigning ports for worker %s\n", filename)

	var worker WorkerConfig
	err := ReadJSONConfig(getConfigPath(filename), &worker)

	if err != nil {
		return err
	}

	port := getUnassignedPortNumber(*assignedPorts)
	worker.WorkerAddr = SetPort(worker.WorkerAddr, port)
	port = getUnassignedPortNumber(*assignedPorts)
	worker.WorkerListenAddr = SetPort(worker.WorkerListenAddr, port)
	port = getUnassignedPortNumber(*assignedPorts)
	worker.FCheckAckLocalAddress = SetPort(worker.FCheckAckLocalAddress, port)
	return WriteJSONConfig(getConfigPath(filename), worker)
}
