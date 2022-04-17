package bagel

import (
	"net/rpc"
)

// constants are used as msgType for the messages
const (
	PAGE_RANK            = "PageRank"
	SHORTEST_PATH        = "ShortestPath"
	SHORTEST_PATH_SOURCE = "ShortestPathSource"
	SHORTEST_PATH_DEST   = "ShortestPathDestination"
)

type WorkerNode struct {
	WorkerId         uint32
	WorkerAddr       string
	WorkerFCheckAddr string
	WorkerListenAddr string
}

type StartSuperStep struct {
	NumWorkers      uint8
	WorkerDirectory WorkerDirectory
	Query           Query
}

type ProgressSuperStep struct {
	SuperStepNum uint64
	IsCheckpoint bool
}

type ProgressSuperStepResult struct {
	SuperStepNum uint64
	IsCheckpoint bool
	IsActive     bool
	CurrentValue interface{}
}

type RestartSuperStep struct {
	SuperStepNumber uint64
	WorkerDirectory WorkerDirectory
}

type CheckpointMsg struct {
	SuperStepNumber uint64
	WorkerId        uint32
}

type Query struct {
	ClientId  string
	QueryType string   // PageRank or ShortestPath
	Nodes     []uint64 // if PageRank, will have 1 vertex, if shortestpath, will have [start, end]
	Graph     string   // graph to use - will always be google for now
}

type QueryResult struct {
	Query  Query
	Result interface{} // client dynamically casts Result based on Query.QueryType:
	Error  string
	// float64 for pagerank, int for shortest path
}

// WorkerDirectory maps worker ids to address (string)
type WorkerDirectory map[uint32]string

// WorkerCallBook maps worker ids to rpc clients (connections)
type WorkerCallBook map[uint32]*rpc.Client
