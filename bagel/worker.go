package bagel

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	database "project/database"
	fchecker "project/fcheck"
	"project/util"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

// Message represents an arbitrary message sent during calculation
// value has a dynamic type based on the messageType
type Message struct {
	SuperStepNum   uint64
	SourceVertexId uint64
	DestVertexId   uint64
	Value          interface{}
}

type WorkerConfig struct {
	WorkerId              uint32
	CoordAddr             string
	WorkerAddr            string
	WorkerListenAddr      string
	FCheckAckLocalAddress string
}

type Worker struct {
	// Worker state may go here
	config          WorkerConfig
	SuperStep       *SuperStep
	NextSuperStep   *SuperStep
	Vertices        map[uint64]Vertex
	workerDirectory WorkerDirectory
	workerCallBook  WorkerCallBook
	NumWorkers      uint32
	QueryType       string
	QueryVertex     uint64
}

type Checkpoint struct {
	SuperStepNumber uint64
	CheckpointState map[uint64]VertexCheckpoint
}

type SuperStep struct {
	Id           uint64
	QueryType    string
	Messages     map[uint64][]Message
	Outgoing     map[uint32][]Message
	IsCheckpoint bool
}

type BatchedMessages struct {
	Batch []Message
}

func NewWorker(config WorkerConfig) *Worker {
	return &Worker{
		config:         config,
		SuperStep:      NewSuperStep(0),
		NextSuperStep:  NewSuperStep(1),
		Vertices:       make(map[uint64]Vertex),
		workerCallBook: make(WorkerCallBook),
	}
}

func (w *Worker) startFCheckHBeat(workerId uint32, ackAddress string) string {
	log.Printf("StartFCheckHBeat for worker %d\n", workerId)

	fcheckConfig := fchecker.StartStruct{
		AckLocalIPAckLocalPort: ackAddress,
	}

	_, addr, err := fchecker.Start(fcheckConfig)

	if err != nil {
		fchecker.Stop()
		util.CheckErr(
			err, "fchecker for Worker %d failed", workerId,
		)
	}

	return addr
}

func NewSuperStep(number uint64) *SuperStep {
	return &SuperStep{
		Id:           number,
		QueryType:    "",
		Messages:     make(map[uint64][]Message),
		Outgoing:     make(map[uint32][]Message),
		IsCheckpoint: false,
	}
}

func (w *Worker) StartQuery(
	startSuperStep StartSuperStep, reply *interface{},
) error {

	w.NumWorkers = uint32(startSuperStep.NumWorkers)
	w.workerDirectory = startSuperStep.WorkerDirectory
	w.QueryType = startSuperStep.QueryType
	w.QueryVertex = startSuperStep.QueryVertex

	log.Printf(
		"StartQuery: worker %v received worker directory: %v\n",
		w.config.WorkerId, w.workerDirectory,
	)

	// workers need to connect to the db and initialize state
	log.Printf(
		"StartQuery: worker %v connecting to db from %v\n", w.config.WorkerId,
		w.config.WorkerAddr,
	)

	//w.Vertices = w.mockVertices(10)

	vertices, err := database.GetVerticesModulo(
		w.config.WorkerId, startSuperStep.NumWorkers,
	)
	if err != nil {
		panic("getVerticesModulo failed")
	}

	for _, v := range vertices {
		var pianoVertex Vertex
		if w.QueryType == SHORTEST_PATH {
			pianoVertex = *NewShortestPathVertex(
				v.VertexID, v.Neighbors, math.MaxInt64,
			)
		} else {
			pianoVertex = *NewPageRankVertex(v.VertexID, v.Neighbors)
		}
		w.Vertices[v.VertexID] = pianoVertex
	}
	fmt.Printf("vertices of worker: %v\n", len(w.Vertices))
	return nil
}

// restore state of the last saved checkpoint
func (w *Worker) RevertToLastCheckpoint(
	req RestartSuperStep, reply *RestartSuperStep,
) error {
	log.Printf(
		"RevertToLastCheckpoint: worker %v received %v\n", w.config.WorkerId,
		req,
	)
	checkpoint, err := w.retrieveCheckpoint(req.SuperStepNumber)
	if err != nil {
		log.Printf(
			"RevertToLastCheckpoint: error retrieving checkpoint: %v\n", err,
		)
		return err
	}
	log.Printf("RevertToLastCheckpoint: retrieved checkpoint: %v\n", checkpoint)

	w.SuperStep.Id = checkpoint.SuperStepNumber
	for k, v := range w.Vertices {
		if state, found := checkpoint.CheckpointState[v.Id]; found {
			log.Printf("RevertToLastCheckpoint: found state: %v\n", state)
			v.currentValue = state.CurrentValue
			v.isActive = state.IsActive
			v.messages = state.Messages
			w.Vertices[k] = v
		}
	}
	log.Printf(
		"RevertToLastCheckpoint: vertices of worker %v: %v\n",
		w.config.WorkerId, w.Vertices,
	)

	*reply = req
	return nil
}

func (w *Worker) listenCoord(handler *rpc.Server) {
	listenAddr, err := net.ResolveTCPAddr("tcp", w.config.WorkerListenAddr)
	util.CheckErr(
		err,
		"Worker %v could not resolve WorkerListenAddr: %v", w.config.WorkerId,
		w.config.WorkerListenAddr,
	)
	listen, err := net.ListenTCP("tcp", listenAddr)
	util.CheckErr(
		err,
		"Worker %v could not listen on listenAddr: %v", w.config.WorkerId,
		listenAddr,
	)

	for {
		conn, err := listen.Accept()
		util.CheckErr(
			err,
			"Worker %v could not accept connections\n", w.config.WorkerId,
		)
		go handler.ServeConn(conn)
	}
}

// create a new RPC Worker instance for the current Worker

func (w *Worker) register() {
	handler := rpc.NewServer()
	err := handler.Register(w)
	log.Printf(
		"register: Worker %v - register error: %v\n", w.config.WorkerId, err,
	)

	go w.listenCoord(handler)
}

func (w *Worker) Start() error {
	// set Worker state
	if w.config.WorkerAddr == "" {
		return errors.New("Failed to start worker. Please initialize worker before calling Start")
	}

	// register Worker for RPC
	w.register()

	// connect to the coord node
	conn, err := util.DialTCPCustom(
		w.config.WorkerAddr, w.config.CoordAddr,
	)

	util.CheckErr(
		err,
		"Worker %d failed to Dial Coordinator - %s\n", w.config.WorkerId,
		w.config.CoordAddr,
	)

	defer conn.Close()
	coordClient := rpc.NewClient(conn)

	hBeatAddr := w.startFCheckHBeat(
		w.config.WorkerId, w.config.FCheckAckLocalAddress,
	)
	log.Printf(
		"Start: hBeatAddr for Worker %d is %v\n", w.config.WorkerId, hBeatAddr,
	)

	workerNode := WorkerNode{
		w.config.WorkerId, w.config.WorkerAddr,
		hBeatAddr, w.config.WorkerListenAddr,
	}

	var response WorkerNode
	err = coordClient.Call("Coord.JoinWorker", workerNode, &response)
	util.CheckErr(
		err, "Start: Worker %v could not join\n", w.config.WorkerId,
	)

	log.Printf(
		"Start: worker %v joined to coord successfully\n",
		w.config.WorkerId,
	)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// setup local checkpoints storage for the worker
	err = w.initializeCheckpoints()
	util.CheckErr(
		err, "Start: Worker %v could not setup checkpoints db\n",
		w.config.WorkerId,
	)

	// go wait for work to do
	wg.Wait()

	return nil
}

func (w *Worker) ComputeVertices(
	args ProgressSuperStep, resp *ProgressSuperStepResult,
) error {
	log.Printf("ComputeVertices - worker %v\n", w.config.WorkerId)

	currentValue := UNUSED_VALUE

	w.updateVerticesWithNewStep(args.SuperStepNum)
	pendingMsgsExist := len(w.SuperStep.Messages) != 0
	allVerticesInactive := true

	for _, vertex := range w.Vertices {
		messages := vertex.Compute(w.QueryType)
		w.updateOutgoingMessages(messages)
		if vertex.isActive {
			allVerticesInactive = false
		}
		// if the current vertex is the source vertex, capture its value
		if vertex.Id == w.QueryVertex {
			currentValue = vertex.currentValue.(float64)
		}
	}

	log.Printf(
		"ComputeVertices: Worker Pending Msgs Status: %v, Worker All Vertices Inactive: %v\n",
		pendingMsgsExist, allVerticesInactive,
	)

	if args.IsCheckpoint {
		checkpoint := w.checkpoint()
		_, err := w.storeCheckpoint(checkpoint)
		util.CheckErr(
			err,
			"Worker %v failed to checkpoint # %v\n", w.config.WorkerId,
			w.SuperStep.Id,
		)
	}

	for worker, msgs := range w.SuperStep.Outgoing {
		if worker == w.config.WorkerId {
			w.SuperStep.Outgoing[worker] = msgs
			continue
		}

		batch := BatchedMessages{Batch: msgs}
		var unused Message
		// todo change to Go

		if _, exists := w.workerCallBook[worker]; !exists {
			var err error
			w.workerCallBook[worker], err = util.DialRPC(w.workerDirectory[worker])

			if err != nil {
				log.Printf(
					"Worker %v could not establish connection to destination worker %v at addr %v\n",
					w.config.WorkerId, worker, w.workerDirectory[worker],
				)
			}
		}

		err := w.workerCallBook[worker].Call(
			"Worker.PutBatchedMessages", batch, &unused,
		)
		if err != nil {
			log.Printf(
				"ComputeVertices: worker %v could not send messages to worker: %v\n",
				w.config.WorkerId, worker,
			)
		}
		log.Printf(
			"Worker #%v sending %v messages\n", w.config.WorkerId,
			len(batch.Batch),
		)
	}

	if !pendingMsgsExist && allVerticesInactive {
		log.Printf("ComputeVertices: All vertices are inactive - worker is inactive.\n")
	}

	*resp = ProgressSuperStepResult{
		SuperStepNum: w.SuperStep.Id,
		IsCheckpoint: args.IsCheckpoint,
		IsActive:     pendingMsgsExist || !allVerticesInactive,
		CurrentValue: currentValue,
	}

	log.Printf("Worker is active %v\n", resp.IsActive)

	err := w.handleSuperStepDone()

	if err != nil {
		log.Printf("ComputeVertices: err: %v\n", err)
		log.Printf(
			"ComputeVertices: worker %v could not complete superstep # %v\n",
			w.config.WorkerId, w.SuperStep.Id,
		)
	}

	return nil
}

func (w *Worker) updateVerticesWithNewStep(superStepNum uint64) {
	for vId, v := range w.Vertices {
		v.SuperStep = superStepNum
		v.messages = w.SuperStep.Messages[vId]
	}
}

func (w *Worker) PutBatchedMessages(
	batch BatchedMessages, resp *Message,
) error {
	log.Printf(
		"Worker %v received %v messages", w.config.WorkerId, len(batch.Batch),
	)
	for _, msg := range batch.Batch {
		w.NextSuperStep.Messages[msg.DestVertexId] = append(
			w.NextSuperStep.Messages[msg.DestVertexId], msg,
		)
	}

	resp = &Message{}
	return nil
}

func (w *Worker) handleSuperStepDone() error {
	log.Printf(
		"handleSuperStepDone: Worker %v transitioning from SuperStep # %d to SuperStep # %d\n",
		w.config.WorkerId, w.SuperStep.Id, w.NextSuperStep.Id,
	)

	*w.SuperStep = *w.NextSuperStep
	w.NextSuperStep = NewSuperStep(w.SuperStep.Id + 1)
	return nil
}

func (w *Worker) updateOutgoingMessages(msgs []Message) {
	for _, msg := range msgs {
		destWorker := util.HashId(msg.DestVertexId) % uint64(w.NumWorkers)
		w.SuperStep.Outgoing[uint32(destWorker)] = append(
			w.SuperStep.Outgoing[uint32(destWorker)], msg,
		)
	}
}

func (w *Worker) mockVertices(numVertices int) map[uint64]Vertex {
	mocks := make(map[uint64]Vertex)
	termination := numVertices * int(w.NumWorkers)
	for i := int(w.config.WorkerId); i < termination; i += int(w.config.WorkerId) {
		vertexId := uint64(i)
		neighbors := w.mockNeighbors(vertexId)
		mockVertex := Vertex{
			Id:             vertexId,
			neighbors:      neighbors,
			previousValues: nil,
			currentValue:   0,
			messages:       nil,
			isActive:       true,
			SuperStep:      0,
		}
		mocks[vertexId] = mockVertex
	}
	return mocks
}

func (w *Worker) mockNeighbors(vertexId uint64) []uint64 {
	neighbors := make([]uint64, 5)

	numNeighbors := rand.Int() % 10

	for i := 0; i < numNeighbors; i++ {
		neighborId := rand.Uint64()
		if neighborId == vertexId {
			i--
			continue
		}
		neighbors = append(neighbors, neighborId)
	}
	return neighbors
}

func (w *Worker) mockMessages() []Message {
	msgs := make([]Message, 10)

	msg := Message{
		SuperStepNum:   w.SuperStep.Id,
		SourceVertexId: 1,
		DestVertexId:   0,
		Value:          1,
	}
	return append(msgs, msg)
}
