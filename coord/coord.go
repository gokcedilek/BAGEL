package coord

type CoordConfig struct {
	StepsBetweenCheckpoints uint64
}

type WorkerNode struct {
	workerId   uint32
	workerAddr string
	// TODO: add more fields as needed
}

type SuperStepDone struct {
	messagesSent        uint64
	allVerticesInactive bool
}
