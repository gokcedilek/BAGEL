package bagel

import (
	"net/rpc"
)

// constants are used as msgType for the messages
const (
	PAGE_RANK     = "PageRank"
	SHORTEST_PATH = "ShortestPath"
)

// TODO: may be needed for the queryWorkers queue
//type WorkerInfo struct {
//	WorkerId uint32
//}

type WorkerNode struct {
	WorkerId         uint32
	WorkerAddr       string
	WorkerFCheckAddr string
	WorkerListenAddr string
}

type StartSuperStep struct {
	NumWorkers uint8
}

type ProgressSuperStep struct {
	SuperStepNum uint64
	IsCheckpoint bool
	IsActive     bool
}

type SuperStepComplete struct {
	IsActive bool
}

type RestartSuperStep struct {
	SuperStepNumber uint64
}

type CheckpointMsg struct {
	SuperStepNumber uint64
	WorkerId        uint32
}

type WorkerDirectory map[uint32]*rpc.Client
