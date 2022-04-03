package bagel

type WorkerNode struct {
	WorkerId         uint32
	WorkerAddr       string
	WorkerFCheckAddr string
	// TODO: add more fields as needed
}

type CheckpointMsg struct {
	SuperStepNumber uint32
	WorkerId        uint32
}
