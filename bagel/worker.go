package bagel

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	database "project/database"
	fchecker "project/fcheck"
	"project/util"
	"sync"
	"time"

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
	// workerCallBook  WorkerPool
	workerCallBook WorkerCallBook
	Replica        WorkerNode
	ReplicaClient  *rpc.Client
	LogicalId      uint32
	NumWorkers     uint32
	QueryVertex    uint64
	workerMutex    sync.Mutex
	logFile        *os.File
	logger         *log.Logger
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

func (w *Worker) retrieveVertices(
	numWorkers uint8, tableName string,
) {
	w.Vertices = make(map[uint64]*Vertex)
	svc := database.GetDynamoClient()
	// TODO: talk about logical IDs for workers
	vertices, err := database.GetPartitionForWorkerX(
		svc,
		tableName,
		int(numWorkers),
		int(w.LogicalId),
	)
	log.Printf(
		"retrieveVertices: retrieved %v vertices for worker %v from the"+
			" db!\n",
		len(vertices),
		w.config.WorkerId,
	)
	log.Printf("vertices: %v", vertices)
	if err != nil {
		panic("getVerticesModulo failed")
	}

	w.workerMutex.Lock()
	w.SuperStep = NewSuperStep()
	w.NextSuperStep = NewSuperStep()

	for _, v := range vertices {
		var pianoVertex Vertex
		if w.Query.QueryType == SHORTEST_PATH {
			pianoVertex = *NewShortestPathVertex(
				v.ID, v.Edges, math.MaxInt32,
			)
			if IsTargetVertex(v.ID, w.Query.Nodes, SHORTEST_PATH_SOURCE) {
				initialMessage := Message{INITIALIZATION_VERTEX, v.ID, 0}
				w.NextSuperStep.Messages[v.ID] = append(
					w.NextSuperStep.Messages[v.ID], initialMessage,
				)
			}
		} else {
			pianoVertex = *NewPageRankVertex(v.ID, v.Edges)
			initialMessage := Message{INITIALIZATION_VERTEX, v.ID, 0.85}
			w.NextSuperStep.Messages[v.ID] = append(
				w.NextSuperStep.Messages[v.ID], initialMessage,
			)
		}
		w.Vertices[v.ID] = &pianoVertex
	}
	w.workerMutex.Unlock()

	log.Printf(
		"retrieveVertices: created partition of %v vertices for worker"+
			" %v"+
			" from the"+
			" db!\n",
		len(w.Vertices),
		w.LogicalId,
	)
}

func (w *Worker) StartQuery(
	startSuperStep StartSuperStep, reply *interface{},
) error {

	log.Printf("StartQuery: startSuperStep: %v\n", startSuperStep)
	w.NumWorkers = uint32(startSuperStep.NumWorkers)
	w.workerDirectory = startSuperStep.WorkerDirectory
	w.Query = startSuperStep.Query
	w.LogicalId = startSuperStep.WorkerLogicalId
	replicaClient, err := util.DialRPC(startSuperStep.ReplicaAddr)
	w.ReplicaClient = replicaClient

	// setup local checkpoints storage for the worker
	err = w.initializeCheckpoints()
	util.CheckErr(
		err, "StartQuery: Worker %v could not setup checkpoints db\n",
		w.config.WorkerId,
	)

	// workers need to connect to the db and initialize state
	log.Printf(
		"StartQuery: worker %v connecting to db from %v\n",
		w.config.WorkerId,
		w.config.WorkerAddr,
	)

	w.retrieveVertices(
		startSuperStep.NumWorkers, startSuperStep.Query.TableName,
	)

	// create a log file
	w.logFile, err = os.OpenFile(
		fmt.Sprintf("worker%v.log", w.config.WorkerId),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	w.logger = log.New(
		w.logFile, fmt.Sprintf(
			"Worker%v ",
			w.config.WorkerId,
		), log.LstdFlags,
	)

	return nil
}

//func (w *Worker) HandleFailover(
//	req PromotedWorker, reply *PromotedWorker,
//) error {
//	// todo implementation
//	w.workerCallBook[req.LogicalId] = req.Worker
//
//	*reply = req
//	return nil
//}

//func (w *Worker) UpdateReplica(
//	req PromotedWorker, reply *PromotedWorker,
//) error {
//	w.Replica = req.Worker
//	*reply = req
//	return nil
//}

func (w *Worker) RevertToLastCheckpoint(
	req RestartSuperStep, reply *RestartSuperStep,
) error {
	w.NumWorkers = uint32(req.NumWorkers)
	w.UpdateWorkerCallBook(req.WorkerDirectory)
	w.workerCallBook = make(WorkerCallBook)
	w.Query = req.Query

	log.Printf(
		"running RevertToLastCheckpoint with superstep number: %v\n",
		req.SuperStepNumber,
	)

	// if failed before saving a checkpoint, revert back to initial state
	if req.SuperStepNumber == 0 {
		log.Printf(
			"RevertToLastCheckpoint: worker %v reverting back to"+
				" initial state with %v workers\n",
			w.config.WorkerId, w.NumWorkers,
		)
		w.retrieveVertices(req.NumWorkers, req.Query.TableName)
		*reply = req
		return nil
	}

	checkpoint, err := w.retrieveCheckpoint(req.SuperStepNumber)

	if err != nil {
		log.Printf(
			"RevertToLastCheckpoint: error retrieving checkpoint: %v"+
				"\n", err,
		)
		return err
	}

	// set vertex state
	w.Vertices = make(map[uint64]*Vertex)
	for k, v := range checkpoint.CheckpointState {
		w.Vertices[k] = &Vertex{
			Id:             v.Id,
			Neighbors:      v.Neighbors,
			PreviousValues: v.PreviousValues,
			CurrentValue:   v.CurrentValue,
		}
	}

	// set superstep state
	w.workerMutex.Lock()
	w.NextSuperStep = &SuperStep{
		Messages:     checkpoint.NextSuperStepState.Messages,
		Outgoing:     checkpoint.NextSuperStepState.Outgoing,
		IsCheckpoint: checkpoint.NextSuperStepState.IsCheckpoint,
	}
	w.workerMutex.Unlock()

	*reply = req
	return nil
}

func (w *Worker) listenCoord(handler *rpc.Server) {
	listenAddr, err := net.ResolveTCPAddr("tcp", w.config.LocalWorkerListAddr)
	util.CheckErr(
		err,
		"listenCoord: worker %v could not resolve WorkerListenAddr"+
			": %v",
		w.config.WorkerId,
		w.config.WorkerListenAddr,
	)
	listen, err := net.ListenTCP("tcp", listenAddr)
	util.CheckErr(
		err,
		"listenCoord: worker %v could not listen on listenAddr: %v",
		w.config.WorkerId,
		listenAddr,
	)

	for {
		conn, err := listen.Accept()
		util.CheckErr(
			err,
			"listenCoord: worker %v could not accept connections\n",
			w.config.WorkerId,
		)
		go handler.ServeConn(conn)
	}
}

// create a new RPC Worker instance for the current Worker

func (w *Worker) register() {
	handler := rpc.NewServer()
	err := handler.Register(w)

	if err != nil {
		log.Printf(
			"register error: %v\n",
			err,
		)
	}

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

	util.CheckErr(
		err,
		"Start: worker %d failed to Dial Coordinator - %s\n",
		w.config.WorkerId,
		w.config.CoordAddr,
	)

	defer conn.Close()
	coordClient := rpc.NewClient(conn)

	hBeatAddr := w.startFCheckHBeat(
		w.config.WorkerId, w.config.LocalFCheckAckLocalAddress,
	)
	log.Printf(
		"Start: hBeatAddr for Worker %d is %v\n", w.config.WorkerId,
		hBeatAddr,
	)

	workerNode := WorkerNode{
		w.config.WorkerId, math.MaxInt32, w.config.WorkerAddr,
		w.config.FCheckAckLocalAddress, w.config.WorkerListenAddr, false,
	}
	// todo discuss with Ryan -- we cant pass nil through rpc,
	// should we just rely on workerdirectory to hold rpc clients?

	var response WorkerNode
	err = coordClient.Call("Coord.JoinWorker", workerNode, &response)
	util.CheckErr(
		err, "Start: Worker %v could not join with error: %v\n",
		w.config.WorkerId, err,
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

func (w *Worker) EndQuery(req EndQuery, reply *EndQuery) error {
	// TODO shut down resources
	log.Printf("Worker %v in endQuery\n", w.LogicalId)
	w.logger = nil
	w.logFile.Close()

	*reply = req
	return nil
}

func (w *Worker) ComputeVertices(
	args *ProgressSuperStep, resp *ProgressSuperStepResult,
) error {
	log.Printf("ComputeVertices: worker: %v, args: %v\n", w, args)

	start := time.Now()

	// save the checkpoint before running superstep S
	if args.IsCheckpoint && !args.IsRestart {
		w.workerMutex.Lock()
		checkpoint := w.checkpoint(args.SuperStepNum)
		_, err := w.storeCheckpoint(checkpoint)
		util.CheckErr(
			err,
			"ComputeVertices: worker %v failed to checkpoint # %v\n",
			w.config.WorkerId,
			args.SuperStepNum,
		)
		log.Printf(
			"ComputeVertices: worker %v saved checkpoint at"+
				" superstep %v\n", w.config.WorkerId, args.SuperStepNum,
		)
		w.workerMutex.Unlock()
	}

	err := w.switchToNextSuperStep()
	if err != nil {
		log.Printf(
			"ComputeVertices: worker %v could not switch to superstep"+
				" %v",
			w.config.WorkerId, args.SuperStepNum,
		)
	}

	hasActiveVertex := false
	for _, vertex := range w.Vertices {
		vertex.SetSuperStepInfo(w.SuperStep.Messages[vertex.Id])
		if len(vertex.Messages) > 0 {
			messages := vertex.Compute(w.Query.QueryType)
			w.mapMessagesToWorkers(messages)
			if vertex.IsActive {
				hasActiveVertex = true
			}
		}

		vertexType := PAGE_RANK
		if w.Query.QueryType == SHORTEST_PATH {
			vertexType = SHORTEST_PATH_DEST
		}

		// if the current vertex is the source vertex, capture its value
		if IsTargetVertex(vertex.Id, w.Query.Nodes, vertexType) {
			log.Printf(
				"ComputeVertices: target vertex %v is on"+
					" worker %v with value %v at superstep %v\n",
				vertex.Id, w.config.WorkerId, vertex.CurrentValue,
				args.SuperStepNum,
			)
			resp.CurrentValue = vertex.CurrentValue

			w.logger.Printf(
				"Completed computation with result %v\n", resp.CurrentValue,
			)
		}
	}

	for worker, msgs := range w.SuperStep.Outgoing {
		if worker == w.LogicalId {
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
			// todo
			w.workerCallBook[worker], err = util.DialRPC(w.workerDirectory[worker])

			if err != nil {
				log.Printf(
					"ComputeVertices: worker %v could not establish"+
						" connection to destination worker %v at addr %v\n",
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
				"ComputeVertices: worker %v could not send messages"+
					" to worker: %v\n",
				w.config.WorkerId, worker,
			)
		}
		log.Printf(
			"ComputeVertices: worker #%v sending %v messages\n",
			w.config.WorkerId,
			len(batch.Batch),
		)
	}

	resp.SuperStepNum = args.SuperStepNum
	resp.IsCheckpoint = args.IsCheckpoint
	resp.IsActive = hasActiveVertex && (w.Query.QueryType != PAGE_RANK || args.SuperStepNum < MAX_ITERATIONS)

	duration := time.Since(start)
	w.logger.Printf(
		"Compute superstep %v took %v s\n", resp.SuperStepNum,
		duration.Seconds(),
	)

	return nil
}

//func (w *Worker) SyncReplica(checkpoint Checkpoint, res *Checkpoint) error {
//	_, err := w.storeCheckpoint(checkpoint)
//	util.CheckErr(err, "Failed to store checkpoint for replica")
//}

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
		"PutBatchedMessages: worker %v received %v messages",
		w.config.WorkerId, len(batch.Batch),
	)
	w.workerMutex.Unlock()

	resp = &Message{}
	return nil
}

func (w *Worker) switchToNextSuperStep() error {
	w.workerMutex.Lock()
	w.SuperStep = w.NextSuperStep
	w.NextSuperStep = NewSuperStep()
	w.workerMutex.Unlock()
	return nil
}

func (w *Worker) mapMessagesToWorkers(msgs []Message) {
	w.workerMutex.Lock()
	for _, msg := range msgs {
		log.Printf("worker %v message: %v\n", w.config.WorkerId, msg)
		destWorker := util.GetFlooredModulo(
			util.HashId(msg.DestVertexId), uint64(w.NumWorkers),
		)
		log.Printf("dst worker: %v\n", destWorker)
		w.SuperStep.Outgoing[uint32(destWorker)] = append(
			w.SuperStep.Outgoing[uint32(destWorker)], msg,
		)
	}
	w.workerMutex.Unlock()
}

func (w *Worker) UpdateWorkerCallBook(newDirectory WorkerDirectory) {
	for workerId, workerAddr := range newDirectory {
		if w.workerDirectory[workerId] != workerAddr {
			w.workerDirectory[workerId] = workerAddr
			delete(w.workerCallBook, workerId)
		}
	}
}
