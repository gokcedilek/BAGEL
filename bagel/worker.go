package bagel

import (
	"errors"
	"fmt"
	"log"
	"math"
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
	SourceVertexId uint64
	DestVertexId   uint64
	Value          interface{}
}

type WorkerConfig struct {
	WorkerId                   uint32
	CoordAddr                  string
	WorkerAddr                 string
	WorkerListenAddr           string
	FCheckAckLocalAddress      string
	LocalWorkerAddr            string
	LocalWorkerListAddr        string
	LocalFCheckAckLocalAddress string
}

type Worker struct {
	// Worker state may go here
	config          WorkerConfig
	SuperStep       *SuperStep
	NextSuperStep   *SuperStep
	Query           Query
	Vertices        map[uint64]*Vertex
	workerDirectory WorkerDirectory
	workerCallBook  WorkerCallBook
	NumWorkers      uint32
	QueryVertex     uint64
	workerMutex     sync.Mutex
}

type SuperStep struct {
	Messages     map[uint64][]Message
	Outgoing     map[uint32][]Message
	IsCheckpoint bool
}

type BatchedMessages struct {
	Batch []Message
}

func NewWorker(config WorkerConfig) *Worker {

	config.LocalWorkerListAddr = util.IPEmptyPortOnly(config.WorkerListenAddr)
	config.LocalWorkerAddr = util.IPEmptyPortOnly(config.WorkerAddr)
	config.LocalFCheckAckLocalAddress = util.IPEmptyPortOnly(config.FCheckAckLocalAddress)

	return &Worker{
		config:          config,
		SuperStep:       NewSuperStep(),
		NextSuperStep:   NewSuperStep(),
		Vertices:        make(map[uint64]*Vertex),
		workerCallBook:  make(map[uint32]*rpc.Client),
		workerDirectory: make(map[uint32]string),
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

func NewSuperStep() *SuperStep {
	return &SuperStep{
		Messages:     make(map[uint64][]Message),
		Outgoing:     make(map[uint32][]Message),
		IsCheckpoint: false,
	}
}

func (w *Worker) retrieveVertices(numWorkers uint8) {
	vertices, err := database.GetVerticesModulo(
		w.config.WorkerId, numWorkers,
	)
	fmt.Printf(
		"retrieved %v vertices for worker %v from the db!\n", len(vertices),
		w.config.WorkerId,
	)
	if err != nil {
		panic("getVerticesModulo failed")
	}
	w.SuperStep = NewSuperStep()
	w.NextSuperStep = NewSuperStep()

	for _, v := range vertices {
		var pianoVertex Vertex
		if w.Query.QueryType == SHORTEST_PATH {
			pianoVertex = *NewShortestPathVertex(
				v.VertexID, v.Neighbors, math.MaxInt32,
			)
			if IsTargetVertex(v.VertexID, w.Query.Nodes, SHORTEST_PATH_SOURCE) {
				initialMessage := Message{INITIALIZATION_VERTEX, v.VertexID, 0}
				w.NextSuperStep.Messages[v.VertexID] = append(
					w.NextSuperStep.Messages[v.VertexID], initialMessage,
				)
			}
		} else {
			pianoVertex = *NewPageRankVertex(v.VertexID, v.Neighbors)
			initialMessage := Message{INITIALIZATION_VERTEX, v.VertexID, 0.85}
			w.NextSuperStep.Messages[v.VertexID] = append(
				w.NextSuperStep.Messages[v.VertexID], initialMessage,
			)
		}
		w.Vertices[v.VertexID] = &pianoVertex
	}
	log.Printf("total # vertices belonging to worker: %v\n", len(w.Vertices))
}

func (w *Worker) StartQuery(
	startSuperStep StartSuperStep, reply *interface{},
) error {

	w.NumWorkers = uint32(startSuperStep.NumWorkers)
	w.workerDirectory = startSuperStep.WorkerDirectory
	w.Query = startSuperStep.Query

	// setup local checkpoints storage for the worker
	err := w.initializeCheckpoints()
	util.CheckErr(
		err, "Start: Worker %v could not setup checkpoints db\n",
		w.config.WorkerId,
	)

	// workers need to connect to the db and initialize state
	log.Printf(
		"StartQuery: worker %v connecting to db from %v\n", w.config.WorkerId,
		w.config.WorkerAddr,
	)

	w.retrieveVertices(startSuperStep.NumWorkers)
	return nil
}

func (w *Worker) RevertToLastCheckpoint(
	req RestartSuperStep, reply *RestartSuperStep,
) error {
	fmt.Printf(
		"RevertToLastCheckpoint: worker %v worker directory: %v\n",
		w.config.WorkerId, req.WorkerDirectory,
	)
	w.NumWorkers = uint32(req.NumWorkers)
	w.UpdateWorkerCallBook(req.WorkerDirectory)
	w.workerCallBook = make(map[uint32]*rpc.Client)
	w.Query = req.Query

	log.Printf(
		"RevertToLastCheckpoint: worker %v received %v\n", w.config.WorkerId,
		req,
	)

	// if failed before saving a checkpoint, revert back to initial state
	if req.SuperStepNumber == 0 {
		log.Printf(
			"RevertToLastCheckpoint: worker %v reverting back to initial state with %v workers\n",
			w.config.WorkerId, w.NumWorkers,
		)
		w.retrieveVertices(req.NumWorkers)
		*reply = req
		return nil
	}

	checkpoint, err := w.retrieveCheckpoint(req.SuperStepNumber)

	if err != nil {
		log.Printf(
			"RevertToLastCheckpoint: error retrieving checkpoint: %v\n", err,
		)
		return err
	}
	log.Printf(
		"RevertToLastCheckpoint: superstep number: %v\n", req.SuperStepNumber,
	)

	// set vertex state
	w.Vertices = make(map[uint64]*Vertex)
	for k, v := range checkpoint.CheckpointState {
		w.Vertices[k] = &Vertex{
			Id:             v.Id,
			Neighbors:      v.Neighbors,
			PreviousValues: v.PreviousValues,
			CurrentValue:   v.CurrentValue,
			Messages:       v.Messages,
			IsActive:       true,
		}
	}

	// set superstep state
	w.NextSuperStep = &SuperStep{
		Messages:     checkpoint.NextSuperStepState.Messages,
		Outgoing:     checkpoint.NextSuperStepState.Outgoing,
		IsCheckpoint: checkpoint.NextSuperStepState.IsCheckpoint,
	}

	*reply = req
	return nil
}

func (w *Worker) listenCoord(handler *rpc.Server) {
	listenAddr, err := net.ResolveTCPAddr("tcp", w.config.LocalWorkerListAddr)
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
		w.config.LocalWorkerAddr, w.config.CoordAddr,
	)

	if err != nil {
		fmt.Println(err)
	}

	util.CheckErr(
		err,
		"Worker %d failed to Dial Coordinator - %s\n", w.config.WorkerId,
		w.config.CoordAddr,
	)

	defer conn.Close()
	coordClient := rpc.NewClient(conn)

	hBeatAddr := w.startFCheckHBeat(
		w.config.WorkerId, w.config.LocalFCheckAckLocalAddress,
	)
	log.Printf(
		"Start: hBeatAddr for Worker %d is %v\n", w.config.WorkerId, hBeatAddr,
	)

	workerNode := WorkerNode{
		w.config.WorkerId, w.config.WorkerAddr,
		w.config.FCheckAckLocalAddress, w.config.WorkerListenAddr,
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

	// go wait for work to do
	wg.Wait()

	return nil
}

func (w *Worker) ComputeVertices(
	args *ProgressSuperStep, resp *ProgressSuperStepResult,
) error {

	// save the checkpoint before running superstep S
	if args.IsCheckpoint && !args.IsRestart {
		w.workerMutex.Lock()
		checkpoint := w.checkpoint(args.SuperStepNum)
		_, err := w.storeCheckpoint(checkpoint)
		util.CheckErr(
			err,
			"Worker %v failed to checkpoint # %v\n", w.config.WorkerId,
			args.SuperStepNum,
		)
		w.workerMutex.Unlock()
	}

	err := w.switchToNextSuperStep()
	if err != nil {
		log.Printf(
			"ComputeVertices: worker %v could not switch to superstep %v",
			w.config.WorkerId, args.SuperStepNum,
		)
	}

	log.Printf(
		"ComputeVertices - worker %v superstep %v, superstep msg length: %v, next superstep msg length: %v\n",
		w.config.WorkerId, args.SuperStepNum, len(w.SuperStep.Messages),
		len(w.NextSuperStep.Messages),
	)

	hasActiveVertex := false
	for _, vertex := range w.Vertices {
		vertex.SetSuperStepInfo(w.SuperStep.Messages[vertex.Id])
		if len(vertex.Messages) > 0 {
			messages := vertex.Compute(w.Query.QueryType, args.IsRestart)
			w.mapMessagesToWorkers(messages)
			if vertex.IsActive {
				hasActiveVertex = true
			}
		}

		// if args.IsRestart {
		// 	log.Printf("ComputeVertices - vertex: %v, message length: %v\n", vertex.Id, len(vertex.Messages))
		// }
		vertexType := PAGE_RANK
		if w.Query.QueryType == SHORTEST_PATH {
			vertexType = SHORTEST_PATH_DEST
		}

		// if the current vertex is the source vertex, capture its value
		if IsTargetVertex(vertex.Id, w.Query.Nodes, vertexType) {
			log.Printf(
				"ComputeVertices -- found TARGET vertex %v on worker %v with value %v at superstep %v\n",
				vertex.Id, w.config.WorkerId, vertex.CurrentValue,
				args.SuperStepNum,
			)
			resp.CurrentValue = vertex.CurrentValue
		}
	}

	for worker, msgs := range w.SuperStep.Outgoing {
		if worker == w.config.WorkerId {
			w.workerMutex.Lock()
			for _, msg := range msgs {
				w.NextSuperStep.Messages[msg.DestVertexId] = append(
					w.NextSuperStep.Messages[msg.DestVertexId], msg,
				)
			}
			w.workerMutex.Unlock()
			continue
		}

		batch := BatchedMessages{Batch: msgs}

		if _, exists := w.workerCallBook[worker]; !exists {
			var err error
			w.workerCallBook[worker], err = util.DialRPC(w.workerDirectory[worker])

			if err != nil {
				log.Printf(
					"Worker %v could not establish connection to destination worker %v at addr %v\n",
					w.config.WorkerId, worker, w.workerDirectory[worker],
				)
				continue
			}
		}

		var unused Message
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

	resp.SuperStepNum = args.SuperStepNum
	resp.IsCheckpoint = args.IsCheckpoint
	resp.IsActive = hasActiveVertex && (w.Query.QueryType != PAGE_RANK || args.SuperStepNum < MAX_ITERATIONS)

	log.Printf(
		"Should notify Coord active for ssn %d = %v, %v\n", args.SuperStepNum,
		resp.IsActive, resp,
	)

	return nil
}

func (w *Worker) PutBatchedMessages(
	batch BatchedMessages, resp *Message,
) error {
	w.workerMutex.Lock()
	for _, msg := range batch.Batch {
		w.NextSuperStep.Messages[msg.DestVertexId] = append(
			w.NextSuperStep.Messages[msg.DestVertexId], msg,
		)
	}
	log.Printf(
		"Worker %v received %v messages", w.config.WorkerId, len(batch.Batch),
	)
	w.workerMutex.Unlock()

	resp = &Message{}
	return nil
}

func (w *Worker) switchToNextSuperStep() error {
	log.Printf(
		"switchToNextSuperStep: Worker %v progressing to next superstep\n",
		w.config.WorkerId,
	)

	w.SuperStep = w.NextSuperStep
	w.NextSuperStep = NewSuperStep()
	return nil
}

func (w *Worker) mapMessagesToWorkers(msgs []Message) {
	for _, msg := range msgs {
		destWorker := util.GetFlooredModulo(
			util.HashId(msg.DestVertexId), int64(w.NumWorkers),
		)
		w.SuperStep.Outgoing[uint32(destWorker)] = append(
			w.SuperStep.Outgoing[uint32(destWorker)], msg,
		)
	}
}

func (w *Worker) UpdateWorkerCallBook(newDirectory WorkerDirectory) {
	for workerId, workerAddr := range newDirectory {
		if w.workerDirectory[workerId] != workerAddr {
			log.Printf(
				"Found outdated address for worker %v; updating to new address %v",
				workerId, workerAddr,
			)
			w.workerDirectory[workerId] = workerAddr
			delete(w.workerCallBook, workerId)
		}
	}
}
