package bagel

// constants are used as msgType for the messages
const (
	PAGE_RANK     = "PageRank"
	SHORTEST_PATH = "ShortestPath"
)

type WorkerNode struct {
	WorkerId         uint32
	WorkerAddr       string
	WorkerFCheckAddr string
	// TODO: add more fields as needed
}

type CheckpointMsg struct {
	SuperStepNumber uint64
	WorkerId        uint32
}
