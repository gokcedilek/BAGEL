package bagel

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
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
	SuperStep       SuperStep
	NextSuperStep   SuperStep
	Vertices        map[uint64]Vertex
	workerDirectory WorkerDirectory
	workerCallBook  WorkerCallBook
	NumWorkers      uint32
	QueryType       string
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
		config: config,
		SuperStep: SuperStep{
			Id:           0,
			Messages:     nil,
			Outgoing:     nil,
			IsCheckpoint: false,
		},
		Vertices: make(map[uint64]Vertex),
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

func (w *Worker) StartQuery(
	startSuperStep StartSuperStep, reply *interface{},
) error {

	w.NumWorkers = uint32(startSuperStep.NumWorkers)
	w.workerDirectory = startSuperStep.WorkerDirectory

	log.Printf(
		"StartQuery: worker %v received worker directory: %v\n",
		w.config.WorkerId, w.workerDirectory)

	// workers need to connect to the db and initialize state
	log.Printf(
		"StartQuery: worker %v connecting to db from %v\n", w.config.WorkerId,
		w.config.WorkerAddr,
	)

	w.mockVertices(10)
	/* TODO mocking vertex since db is too large;

	vertices, err := database.GetVerticesModulo(w.config.WorkerId, startSuperStep.NumWorkers)
	if err != nil {
		panic("getVerticesModulo failed")
	}

	for _, v := range vertices {
		pianoVertex := Vertex{
			Id:           v.VertexID,
			neighbors:    v.Neighbors,
			currentValue: 0,
			messages:     nil,
			isActive:     false,
			SuperStep:    0,
		}
		w.Vertices[v.VertexID] = pianoVertex
	}
	*/
	fmt.Printf("vertices of worker: %v\n", w.Vertices)
	return nil
}

func checkpointsSetup() (*sql.DB, error) {
	//goland:noinspection SqlDialectInspection
	const createCheckpoints string = `
	  CREATE TABLE IF NOT EXISTS checkpoints (
	  lastCheckpointNumber INTEGER NOT NULL PRIMARY KEY, 
-- 	  lastCheckpointNumber INTEGER NOT NULL, // TODO: use this for local setup (to be removed)
	  checkpointState BLOB NOT NULL
	  );`
	db, err := sql.Open("sqlite3", "checkpoints.db")
	if err != nil {
		log.Printf("checkpointsSetup: Failed to open database: %v\n", err)
		return nil, err
	}

	if _, err := db.Exec(createCheckpoints); err != nil {
		log.Printf("checkpointsSetup: Failed execute command: %v\n", err)
		return nil, err
	}

	return db, nil
}

func (w *Worker) checkpoint() Checkpoint {
	checkPointState := make(map[uint64]VertexCheckpoint)

	for k, v := range w.Vertices {
		checkPointState[k] = VertexCheckpoint{
			CurrentValue: v.currentValue,
			Messages:     v.messages,
			IsActive:     v.isActive,
		}
	}

	return Checkpoint{
		SuperStepNumber: w.SuperStep.Id,
		CheckpointState: checkPointState,
	}
}

func (w *Worker) storeCheckpoint(checkpoint Checkpoint) (Checkpoint, error) {
	db, err := checkpointsSetup()
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()

	var buf bytes.Buffer
	if err = gob.NewEncoder(&buf).Encode(checkpoint.CheckpointState); err != nil {
		log.Printf("storeCheckpoint: encode error: %v\n", err)
	}

	_, err = db.Exec(
		"INSERT INTO checkpoints VALUES(?,?)",
		checkpoint.SuperStepNumber,
		buf.Bytes(),
	)
	if err != nil {
		log.Printf("storeCheckpoint: error inserting into db: %v\n", err)
	}

	// notify coord about the latest checkpoint saved
	coordClient, err := util.DialRPC(w.config.CoordAddr)
	util.CheckErr(err,
		"worker %v could not dial coord addr %v\n", w.config.WorkerAddr, w.config.CoordAddr,
	)

	checkpointMsg := CheckpointMsg{
		SuperStepNumber: checkpoint.SuperStepNumber,
		WorkerId:        w.config.WorkerId,
	}

	var reply CheckpointMsg
	log.Printf("storeCheckpoints: calling coord with checkpointMsg: %v\n", checkpointMsg)
	err = coordClient.Call("Coord.UpdateCheckpoint", checkpointMsg, &reply)
	util.CheckErr(err,
		"storeCheckpoints: worker %v could not call UpdateCheckpoint", w.config.WorkerAddr,
	)

	return checkpoint, nil
}

func (w *Worker) retrieveCheckpoint(superStepNumber uint64) (
	Checkpoint, error,
) {
	db, err := checkpointsSetup()
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()

	res := db.QueryRow(
		"SELECT * FROM checkpoints WHERE lastCheckpointNumber=?", superStepNumber,
	)
	checkpoint := Checkpoint{}
	var buf []byte
	if err := res.Scan(
		&checkpoint.SuperStepNumber, &buf,
	); err == sql.ErrNoRows {
		log.Printf("retrieveCheckpoint: scan error: %v\n", err)
		return Checkpoint{}, err
	}

	var checkpointState map[uint64]VertexCheckpoint
	err = gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&checkpointState)
	if err != nil {
		log.Printf("retrieveCheckpoint: decode error: %v, tmp: %v\n", err, checkpointState)
		return Checkpoint{}, err
	}
	checkpoint.CheckpointState = checkpointState

	log.Printf(
		"retrieveCheckpoint: read ssn: %v, state: %v\n", checkpoint.SuperStepNumber,
		checkpoint.CheckpointState,
	)

	return checkpoint, nil
}

// restore state of the last saved checkpoint
func (w *Worker) RevertToLastCheckpoint(
	req RestartSuperStep, reply *RestartSuperStep,
) error {
	log.Printf("RevertToLastCheckpoint: worker %v received %v\n", w.config.WorkerId, req)
	checkpoint, err := w.retrieveCheckpoint(req.SuperStepNumber)
	if err != nil {
		log.Printf("RevertToLastCheckpoint: error retrieving checkpoint: %v\n", err)
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
	log.Printf("RevertToLastCheckpoint: vertices of worker %v: %v\n", w.config.WorkerId, w.Vertices)

	*reply = req
	return nil
}

func (w *Worker) listenCoord(handler *rpc.Server) {
	listenAddr, err := net.ResolveTCPAddr("tcp", w.config.WorkerListenAddr)
	util.CheckErr(err,
		"Worker %v could not resolve WorkerListenAddr: %v", w.config.WorkerId, w.config.WorkerListenAddr,
	)
	listen, err := net.ListenTCP("tcp", listenAddr)
	util.CheckErr(err,
		"Worker %v could not listen on listenAddr: %v", w.config.WorkerId, listenAddr,
	)

	for {
		conn, err := listen.Accept()
		util.CheckErr(err,
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

	util.CheckErr(err,
		"Worker %d failed to Dial Coordinator - %s\n", w.config.WorkerId, w.config.CoordAddr,
	)

	defer conn.Close()
	coordClient := rpc.NewClient(conn)

	hBeatAddr := w.startFCheckHBeat(
		w.config.WorkerId, w.config.FCheckAckLocalAddress,
	)
	log.Printf("Start: hBeatAddr for Worker %d is %v\n", w.config.WorkerId, hBeatAddr)

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

	// TODO: this needs to be tested properly when workers are deployed on different machines
	// begin checkpoints test
	checkpoint0 := Checkpoint{
		SuperStepNumber: 0, CheckpointState: make(map[uint64]VertexCheckpoint),
	}

	checkpoint0.CheckpointState[uint64(1)] = VertexCheckpoint{
		CurrentValue: 1,
		Messages:     nil,
		IsActive:     true,
	}

	checkpoint0.CheckpointState[uint64(2)] = VertexCheckpoint{
		CurrentValue: 2,
		Messages:     nil,
		IsActive:     true,
	}

	checkpoint0, err = w.storeCheckpoint(checkpoint0)
	log.Printf("stored checkpoint0: %v\n", checkpoint0)

	checkpoint1 := Checkpoint{
		SuperStepNumber: 1, CheckpointState: make(map[uint64]VertexCheckpoint),
	}

	checkpoint1.CheckpointState[uint64(3)] = VertexCheckpoint{
		CurrentValue: 3,
		Messages:     nil,
		IsActive:     true,
	}

	checkpoint1.CheckpointState[uint64(4)] = VertexCheckpoint{
		CurrentValue: 4,
		Messages:     nil,
		IsActive:     true,
	}

	checkpoint1, err = w.storeCheckpoint(checkpoint1)
	log.Printf("stored checkpoint1: %v\n", checkpoint1)

	// end checkpoints test

	// go wait for work to do
	wg.Wait()

	return nil
}

func (w *Worker) ComputeVertices(args ProgressSuperStep, resp *ProgressSuperStep) error {
	log.Printf("wComputeVertices - worker %v\n", w.config.WorkerId)

	w.updateVerticesWithNewStep(args.SuperStepNum)
	pendingMsgsExist := len(w.SuperStep.Messages) != 0
	allVerticesInactive := true

	for _, vertex := range w.Vertices {
		messages := vertex.Compute(SHORTEST_PATH) // TODO: get the information about which computation to use
		w.updateMessagesMap(messages)
		if vertex.isActive {
			allVerticesInactive = false
		}
	}

	log.Printf("ComputeVertices: Worker Pending Msgs Status: %v, Worker All Vertices Active: %v\n",
		pendingMsgsExist, allVerticesInactive)

	/* TODO commented out until Vertex Compute() impl.
	if !pendingMsgsExist && allVerticesInactive {
		log.Printf("ComputeVertices: All vertices are inactive - worker is inactive.\n")
	}
	*/

	if args.IsCheckpoint {
		checkpoint := w.checkpoint()
		_, err := w.storeCheckpoint(checkpoint)
		util.CheckErr(err,
			"Worker %v failed to checkpoint # %v\n", w.config.WorkerId, w.SuperStep.Id,
		)
	}

	for worker, msgs := range w.SuperStep.Outgoing {

		// local vertices
		if worker == w.config.WorkerId {
			w.SuperStep.Outgoing[worker] = msgs
			continue
		}

		batch := BatchedMessages{Batch: msgs}
		var unused Message
		// todo change to Go
		err := w.workerCallBook[worker].Call("Worker.PutBatchedMessages", batch, &unused)
		if err != nil {
			log.Printf("ComputeVertices: worker %v could not send messages to worker: %v\n",
				w.config.WorkerId, worker)
		}
	}

	resp = &ProgressSuperStep{
		SuperStepNum: w.SuperStep.Id,
		IsCheckpoint: args.IsCheckpoint,
		IsActive:     !pendingMsgsExist && allVerticesInactive,
	}

	err := w.handleSuperStepDone()

	if err != nil {
		log.Printf("ComputeVertices: err: %v\n", err)
		log.Printf("ComputeVertices: worker %v could not complete superstep # %v\n",
			w.config.WorkerId, w.SuperStep.Id)
	}

	return nil
}

func (w *Worker) updateVerticesWithNewStep(superStepNum uint64) {
	for vId, v := range w.Vertices {
		v.SuperStep = superStepNum
		v.messages = w.SuperStep.Messages[vId]
	}
}

func (w *Worker) PutBatchedMessages(batch BatchedMessages, resp *Message) error {
	for _, msg := range batch.Batch {
		w.NextSuperStep.Messages[msg.DestVertexId] = append(w.NextSuperStep.Messages[msg.DestVertexId], msg)
	}

	resp = &Message{}
	return nil
}

func (w *Worker) handleSuperStepDone() error {

	w.NextSuperStep.Id = w.SuperStep.Id + 1

	log.Printf(
		"handleSuperStepDone: Worker %v transitioning from SuperStep # %d to SuperStep # %d\n",
		w.config.WorkerId, w.SuperStep.Id, w.NextSuperStep.Id,
	)

	w.SuperStep = w.NextSuperStep
	w.NextSuperStep = SuperStep{Id: w.SuperStep.Id + 1}
	return nil
}

func (w *Worker) updateMessagesMap(msgs []Message) {
	for _, msg := range msgs {
		destWorker := util.HashId(msg.DestVertexId) % uint64(w.NumWorkers)
		w.SuperStep.Outgoing[uint32(destWorker)] = append(w.SuperStep.Outgoing[uint32(destWorker)], msg)
	}
}

func (w *Worker) mockVertices(numVertices int) []Vertex {
	mocks := make([]Vertex, 0, 10)
	termination := numVertices * int(w.NumWorkers)
	for i := int(w.config.WorkerId); i < termination; i += int(w.config.WorkerId) {
		vertexId := uint64(i)
		neighbors := w.mockNeighbors(vertexId)
		mockVertex := Vertex{
			Id:             vertexId,
			neighbors:      neighbors,
			previousValues: nil,
			currentValue:   nil,
			messages:       nil,
			isActive:       true,
			SuperStep:      0,
		}
		mocks = append(mocks, mockVertex)
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
