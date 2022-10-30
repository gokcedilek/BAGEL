package util

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
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

func AssignToRemote(coordServer string, clientServer string) error {
	var config map[string]interface{}
	err := ReadJSONConfig("config/remote_configs.json", &config)
	if err != nil {
		return err
	}

	_, coordExists := config[coordServer]
	_, clientExists := config[clientServer]

	if !coordExists || !clientExists {
		return errors.New(
			fmt.Sprintf(
				"invalid coordinator or client address supplied; available servers: %v\n",
				getServerNames(config),
			),
		)
	}

	workerServers := getWorkerServers(coordServer, clientServer, config)
	serverAssignments := make([]string, len(config))

	files, err := os.ReadDir("config")
	if err != nil {
		return err
	}
	for _, file := range files {
		filename := file.Name()

		if isConfigType(filename, COORD) {
			err = reassignCoordHost(filename, "")
			if err != nil {
				return err
			}
			serverAssignments = append(
				serverAssignments,
				getAzureServerAssignmentMsg(
					filename, coordServer, config[coordServer].(string),
				),
			)
		}

		if isConfigType(filename, WORKERS) {
			var serverName, serverAddr string
			for name, addr := range workerServers {
				serverName = name
				serverAddr = addr
				break
			}
			delete(workerServers, serverName)
			if serverName == "" {
				log.Printf(
					"ReassignWorkerHost - ran out of available servers %s\n",
					filename,
				)
				log.Printf("Remote Server Assignments\n%v\n", serverAssignments)
				return nil
			}

			err = reassignWorkerHosts(
				filename, serverAddr, config[coordServer].(string),
			)
			if err != nil {
				return err
			}
			serverAssignments = append(
				serverAssignments,
				getAzureServerAssignmentMsg(filename, serverName, serverAddr),
			)
		}

		if isConfigType(filename, CLIENT) {
			err = reassignClientHost(
				filename, config[clientServer].(string),
				config[coordServer].(string),
			)
			if err != nil {
				return err
			}
			serverAssignments = append(
				serverAssignments,
				getAzureServerAssignmentMsg(
					filename, clientServer, config[clientServer].(string),
				),
			)
		}
	}

	log.Printf("Remote Server Assignments\n%v\n", serverAssignments)
	return nil
}

func getAzureServerAssignmentMsg(
	nodeType string, serverName string, serverAddr string,
) string {
	return fmt.Sprintf(
		"%s assigned to server %s : %s\n", nodeType, serverName, serverAddr,
	)
}

func SetPort(ipPort string, port int) string {
	host, _, err := net.SplitHostPort(ipPort)
	if err != nil {
		log.Fatalf("SetPort - Failed to separate host and port %v\n", err)
		return ""
	}

	return net.JoinHostPort(host, strconv.Itoa(port))
}

func IPEmptyPortOnly(hostport string) string {
	_, port, err := net.SplitHostPort(hostport)
	if err != nil {
		log.Fatalf(
			"IPEmptyPortOnly - Failed to separate host and port %v\\n", err,
		)
		return ""
	}
	return fmt.Sprintf(":%s", port)
}

func SetHost(hostport string, host string) string {
	_, port, err := net.SplitHostPort(hostport)
	if err != nil {
		log.Fatalf("SetHost - failed to split host port %v\n", err)
		return ""
	}
	return net.JoinHostPort(host, port)
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

func getWorkerServers(
	coordServer string, clientServer string, serverMap map[string]interface{},
) map[string]string {
	availableServers := make(map[string]string)
	for server, address := range serverMap {
		if server != coordServer && server != clientServer {
			availableServers[server] = address.(string)
		}
	}
	return availableServers
}

func getServerNames(remoteConfig map[string]interface{}) []string {
	names := make([]string, len(remoteConfig))

	for name := range remoteConfig {
		names = append(names, name)
	}
	return names
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

func reassignCoordHost(filename string, host string) error {
	log.Printf("Reassigning host for coord %s to %s\n", filename, host)

	var coord CoordConfig
	err := ReadJSONConfig(getConfigPath(filename), &coord)

	if err != nil {
		return err
	}

	coord.ClientAPIListenAddr = SetHost(coord.ClientAPIListenAddr, "")
	coord.WorkerAPIListenAddr = SetHost(coord.WorkerAPIListenAddr, "")
	return WriteJSONConfig(getConfigPath(filename), coord)
}

func reassignClientHost(filename string, host string, coordHost string) error {
	log.Printf("Reassigning host for client %s to %s\n", filename, host)

	var client ClientConfig
	err := ReadJSONConfig(getConfigPath(filename), &client)

	if err != nil {
		return err
	}

	client.ClientAddr = SetHost(client.ClientAddr, host)
	client.CoordAddr = SetHost(client.CoordAddr, coordHost)

	return WriteJSONConfig(getConfigPath(filename), client)
}

func reassignWorkerHosts(filename string, host string, coordHost string) error {
	log.Printf("Reassigning host for worker %s to %s\n", filename, host)

	var worker WorkerConfig
	err := ReadJSONConfig(getConfigPath(filename), &worker)

	if err != nil {
		return err
	}

	worker.CoordAddr = SetHost(worker.CoordAddr, coordHost)
	worker.WorkerAddr = SetHost(worker.WorkerAddr, host)
	worker.WorkerListenAddr = SetHost(worker.WorkerListenAddr, host)
	worker.FCheckAckLocalAddress = SetHost(worker.FCheckAckLocalAddress, host)
	return WriteJSONConfig(getConfigPath(filename), worker)
}
