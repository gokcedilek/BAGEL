package bagel

// constants are used as msgType for the messages
const (
	PAGE_RANK     = "PageRank"
	SHORTEST_PATH = "ShortestPath"
)

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

type RestartSuperStep struct {
	SuperStepNumber uint64
}

type CheckpointMsg struct {
	SuperStepNumber uint64
	WorkerId        uint32
}
