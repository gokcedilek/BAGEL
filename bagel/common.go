package bagel

import (
	"net/rpc"
	"math"
)

// constants are used as msgType for the messages
const (
	PAGE_RANK     = "PageRank"
	SHORTEST_PATH = "ShortestPath"
)

const UNUSED_VALUE = math.MaxFloat64

type WorkerNode struct {
	WorkerId         uint32
	WorkerAddr       string
	WorkerFCheckAddr string
	WorkerListenAddr string
}

type StartSuperStep struct {
	NumWorkers      uint8
	WorkerDirectory WorkerDirectory
	QueryType       string
	QueryVertex     uint64 // the target vertex if doing shortest paths and the only vertex if doing pagerank
}

type ProgressSuperStep struct {
	SuperStepNum uint64
	IsCheckpoint bool
	IsActive     bool
}

type ProgressSuperStepResult struct {
	SuperStepNum uint64
	IsCheckpoint bool
	IsActive     bool
	CurrentValue interface{}
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

// WorkerDirectory maps worker ids to address (string)
type WorkerDirectory map[uint32]string

// WorkerCallBook maps worker ids to rpc clients (connections)
type WorkerCallBook map[uint32]*rpc.Client
