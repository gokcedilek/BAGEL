package util

import (
	"fmt"
	"os"
	"strings"
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
	WORKERS = "worker"
	CLIENT  = "client"
)

func SynchronizeConfigs() error {
	files, err := os.ReadDir("config")
	if err != nil {
		return err
	}

	var coord CoordConfig
	err = ReadJSONConfig(GetConfigPath("coord_config.json"), &coord)
	if err != nil {
		return err
	}

	for _, file := range files {
		filename := file.Name()

		if IsClientConfig(filename) {
			var client ClientConfig
			err = ReadJSONConfig(GetConfigPath(filename), &client)
			client.CoordAddr = coord.ClientAPIListenAddr
			err := WriteJSONConfig(GetConfigPath(filename), client)
			if err != nil {
				return err
			}
		}
		if IsWorkerConfig(filename) {
			var worker WorkerConfig
			err = ReadJSONConfig(GetConfigPath(filename), &worker)
			worker.CoordAddr = coord.WorkerAPIListenAddr
			err := WriteJSONConfig(GetConfigPath(filename), worker)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func AssignWorkerIPPorts() error {
	return nil
}

func IsClientConfig(filename string) bool {
	return strings.HasPrefix(filename, CLIENT)
}

func IsWorkerConfig(filename string) bool {
	return strings.HasPrefix(filename, WORKERS)
}

func GetConfigPath(filename string) string {
	return fmt.Sprintf("config/%s", filename)
}