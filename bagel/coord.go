package bagel

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	coordgRPC "project/bagel/proto/coord"
	"project/database/mongodb"
	fchecker "project/fcheck"
	"project/util"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	//"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/grpc"
)

const (
	coordProcesses = 3
)

// this is the start of the query where coord notifies workers to initialize
// state for SuperStep 0
func (c *Coord) StartQuery(ctx context.Context, q *coordgRPC.Query) (
	*coordgRPC.QueryResult,
	error,
) {
	var reply coordgRPC.QueryResult

	log.Printf("StartQuery: workers: %v\n", c.workers)
	log.Printf("StartQuery: received query: %v\n", q)

	if len(c.workers) == 0 {
		log.Printf(
			"StartQuery: No workers available - will block" +
				" until workers join\n",
		)
	}

	for len(c.workers) == 0 {
		// block while no workers available
	}

	// validate vertices sent by the client query
	client := mongodb.GetDatabaseClient()
	collection := mongodb.GetCollection(client, q.TableName)
	for _, vId := range q.Nodes {
		vertex, err := mongodb.GetVertexById(collection, vId)
		if err != nil {
			reply.Error = err.Error()
			return &reply, nil
		}
		log.Printf("coord fetched query vertex %v\n", vertex)
	}

	// todo replace with workerCount from client
	testWorkerCount := 2
	c.assignQueryWorkers(testWorkerCount)

	// initialize workerReady map
	c.workerReadyMap = make(map[uint32]bool)
	for logicalId := range c.queryWorkers {
		c.workerReadyMap[logicalId] = true
	}

	fmt.Printf("query workers: %v\n", c.queryWorkers)
	fmt.Printf("query replicas: %v\n", c.queryReplicas)

	// initialize worker directory

	// create new map of checkpoints for a new query which may have different number of workers
	c.lastCheckpointNumber = 0
	c.lastWorkerCheckpoints = make(map[uint32]uint64)
	c.superStepNumber = 1

	coordQueryType := ""
	switch q.QueryType {
	case coordgRPC.QUERY_TYPE_PAGE_RANK:
		coordQueryType = PAGE_RANK
	case coordgRPC.QUERY_TYPE_SHORTEST_PATH:
		coordQueryType = SHORTEST_PATH
	}

	coordQuery := Query{
		ClientId:  q.ClientId,
		QueryType: coordQueryType,
		Nodes:     q.Nodes,
		Graph:     q.Graph,
		TableName: q.TableName,
	}
	log.Printf("StartQuery: sending query: %v\n", coordQuery)

	startSuperStep := StartSuperStep{
		NumWorkers:            uint8(len(c.queryWorkers)),
		WorkerDirectory:       c.GetWorkerDirectory(),
		Query:                 coordQuery,
		HasReplicaInitialized: false,
	}

	numWorkers := len(c.queryWorkers)
	c.workerDoneStart = make(chan *rpc.Call, numWorkers)
	c.workerDoneCompute = make(chan *rpc.Call, numWorkers)
	c.workerDoneRestart = make(chan *rpc.Call, numWorkers)
	c.workerDoneFailover = make(chan *rpc.Call, numWorkers)
	c.allWorkersReady = make(chan superstepDone, 1)
	c.restartSuperStepCh = make(chan uint32, numWorkers)
	c.queryProgress = make(chan queryProgress, 1)
	c.fetchGraphDone = make(chan WorkerVertices, 1)

	c.query = coordQuery

	log.Printf(
		"StartQuery: computing query %v with %d workers ready!\n", q,
		numWorkers,
	)
	log.Printf(
		"StartQuery: query workers callbook: %v\n", c.queryWorkersCallbook,
	)

	// call workers start query handlers
	var startQueryResult StartSuperStepResult
	for logicalId, client := range c.queryWorkersCallbook {
		//var result StartSuperStepResult
		startSuperStep.WorkerLogicalId = logicalId
		startSuperStep.ReplicaAddr = c.queryReplicas[logicalId].WorkerListenAddr
		client.Go(
			"Worker.StartQuery", startSuperStep, &startQueryResult,
			c.workerDoneStart,
		)
	}

	go c.blockWorkersReady(
		len(c.queryWorkers),
		c.workerDoneStart,
	)

	// create a log file
	logFile, err := os.OpenFile(
		"coord.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	logger := log.New(logFile, "Coord ", log.LstdFlags)

	// start query computation
	result, err := c.Compute(logger)
	if err != nil {
		log.Printf("StartQuery: Compute returned error: %v\n", err)
	}
	log.Printf("StartQuery: computed result: %v\n", result)

	reply.Query = q

	// cast type of result to float64
	//note: we cannot convert interface{} to float64,
	//need to identify the runtime type of interface{} first)
	log.Printf("type of result: %T\n", result)
	switch resultType := result.(type) {
	case float64:
		reply.Result = resultType
	case int:
		reply.Result = float64(resultType)
		//default:
		//	reply.Result = float64(resultType)
	}

	log.Printf("StartQuery: sending back result: %v\n", reply.Result)

	c.queryWorkers = nil
	c.queryReplicas = nil
	c.queryWorkersCallbook = nil
	c.query = Query{}

	// return nil for no errors
	return &reply, nil
}

func (c *Coord) FetchGraph(
	ctx context.Context, req *coordgRPC.FetchGraphRequest,
) (
	*coordgRPC.FetchGraphResponse, error,
) {
	var reply coordgRPC.FetchGraphResponse
	//	workerVertices := make(map[uint32]*coordgRPC.WorkerVertices)
	//fetchGraph:
	//	for {
	//		select {
	//		case workerVerticesDone := <-c.fetchGraphDone:
	//			log.Printf("HELLO FETCHGRAPH RECEIVED: %v\n", workerVerticesDone)
	//			for wId, vertices := range workerVerticesDone {
	//				workerVertices[wId] = &coordgRPC.
	//					WorkerVertices{Vertices: vertices}
	//			}
	//			break fetchGraph
	//		default:
	//		}
	//	}

	//reply.WorkerVertices = workerVertices
	return &reply, nil
}

func (c *Coord) QueryProgress(
	req *coordgRPC.QueryProgressRequest,
	stream coordgRPC.Coord_QueryProgressServer,
) error {
	//streamDoneCh := make(chan error, 1)
	//go c.streamQueryProgress(req, stream, streamDoneCh)
	//
	//select {
	//case done := <-streamDoneCh:
	//	log.Printf("result received from streamQueryProgress: %v\n", done)
	//	return done
	//}
	return nil
}

func (c *Coord) streamQueryProgress(
	req *coordgRPC.QueryProgressRequest,
	stream coordgRPC.Coord_QueryProgressServer,
	done chan error,
) {
	for {
		select {
		case progress := <-c.queryProgress:
			log.Printf("Coord TempSensor read progress: %v\n", progress)

			messages := make(map[uint64]*coordgRPC.VertexMessages)
			for vId, progressMessages := range progress.messages {
				var grpcMessages []*coordgRPC.VertexMessage
				// populate grpcMessages
				for _, msg := range progressMessages {
					val, ok := msg.Value.(int)
					if !ok {
						val = -1
					}
					grpcMessages = append(
						grpcMessages, &coordgRPC.VertexMessage{
							SourceVertexId: msg.SourceVertexId,
							DestVertexId:   msg.DestVertexId,
							Value:          int64(val),
						},
					)
				}
				messages[vId] = &coordgRPC.VertexMessages{VertexMessages: grpcMessages}
			}
			payload := coordgRPC.QueryProgressResponse{
				SuperstepNumber: progress.superstepNumber,
				Messages:        messages,
			}
			err := stream.Send(&payload)
			if err != nil {
				log.Printf("Coord TempSensor error: %v\n", err)
				done <- err
				return
			}
			log.Printf("Coord TempSensor sent payload: %v\n", payload)

			if progress.done {
				log.Printf("Coord TempSensor finished at: %v\n", progress)
				done <- nil
				return
			}
			break
		default:
		}
	}
}

/* end of proto config */

type CoordConfig struct {
	ClientAPIListenAddr   string // client will know this and use it to contact coord
	WorkerAPIListenAddr   string // new joining workers will message this addr
	ExternalAPIListenAddr string // external HTTP endpoint to spin up/down
	// workers
	LostMsgsThresh          uint8 // fcheck
	StepsBetweenCheckpoints uint64
}

type Coord struct {
	// Coord state may go here
	coordgRPC.UnimplementedCoordServer
	clientAPIListenAddr  string
	workerAPIListenAddr  string
	lostMsgsThresh       uint8
	workers              WorkerPool // worker id --> worker connection
	queryWorkers         WorkerPool // workers in use for current query
	queryWorkersCallbook WorkerCallBook
	queryReplicas        WorkerPool // replicas in use for current query
	// - will be updated at start of query
	//queryWorkersDirectory WorkerDirectory // should only include main workers
	//workersDirectory WorkerDirectory
	//queryWorkersFailover  FailoverWorkerCallBook
	//workerPoolFailover    WorkerCallBook
	lastCheckpointNumber  uint64
	lastWorkerCheckpoints map[uint32]uint64
	checkpointFrequency   uint64
	superStepNumber       uint64
	workerDoneStart       chan *rpc.Call // done messages for Worker.StartQuery RPC
	workerDoneCompute     chan *rpc.Call // done messages for Worker.ComputeVertices RPC
	workerDoneRestart     chan *rpc.Call // done messages for Worker.RevertToLastCheckpoint RPC
	workerDoneFailover    chan *rpc.Call
	allWorkersReady       chan superstepDone
	restartSuperStepCh    chan uint32
	query                 Query
	workerReadyMap        map[uint32]bool
	workerReadyMapMutex   sync.Mutex
	mx                    sync.Mutex
	queryProgress         chan queryProgress
	fetchGraphDone        chan WorkerVertices
}

type superstepDone struct {
	allWorkersInactive bool
	isSuccess          bool
	value              interface{}
	isRestart          bool
	// experimental
	messages VertexMessages
	// experimental
	workerVertices WorkerVertices
}

type queryProgress struct {
	superstepNumber uint64
	done            bool
	// experimental
	messages VertexMessages
}

func NewCoord() *Coord {

	return &Coord{
		clientAPIListenAddr:   "",
		workerAPIListenAddr:   "",
		lostMsgsThresh:        0,
		lastWorkerCheckpoints: make(map[uint32]uint64),
		workers:               make(WorkerPool),
		queryWorkers:          make(WorkerPool),
		queryReplicas:         make(WorkerPool),
		queryWorkersCallbook:  make(WorkerCallBook),
		//queryWorkersDirectory:    make(WorkerDirectory),
		//workersDirectory:         make(WorkerDirectory),
		superStepNumber:          1,
		UnimplementedCoordServer: coordgRPC.UnimplementedCoordServer{},
	}
}

func (c *Coord) assignQueryWorkers(workerCount int) {
	// assign logical ids
	// assign main/replica
	c.queryWorkers = make(WorkerPool)
	c.queryReplicas = make(WorkerPool)
	c.queryWorkersCallbook = make(WorkerCallBook)

	if len(c.workers) < workerCount*2 {
		log.Fatalf(
			"Do not have enough workers to perform query. "+
				"Worker count: %v, Desired Worker count: %v\n",
			len(c.workers),
			workerCount,
		)
	}

	log.Printf("ASSIGN main/replica workers: %v\n", c.workers)
	logicalId := uint32(0)
	idx := 0

	// create a sorted iteration order over the map of workers
	configIds := make([]uint32, 0, len(c.workers))
	for id := range c.workers {
		configIds = append(configIds, id)
	}
	sort.Slice(
		configIds, func(i, j int) bool {
			return configIds[i] < configIds[j]
		},
	)

	for _, configId := range configIds {
		workerNode := c.workers[configId]
		if idx%2 == 0 {
			c.queryWorkers[logicalId] = WorkerNode{
				WorkerConfigId:   workerNode.WorkerConfigId,
				WorkerLogicalId:  logicalId,
				WorkerAddr:       workerNode.WorkerAddr,
				WorkerFCheckAddr: workerNode.WorkerFCheckAddr,
				WorkerListenAddr: workerNode.WorkerListenAddr,
				IsReplica:        false,
			}
			fmt.Printf(
				"Worker %v assigned logical id %v as isReplica %v\n",
				workerNode.WorkerConfigId, logicalId, workerNode.IsReplica,
			)
			client, err := util.DialRPC(workerNode.WorkerListenAddr)
			if err != nil {
				log.Fatalf(
					"cannot create client for worker %v addr %v\n",
					workerNode.WorkerConfigId, workerNode.WorkerListenAddr,
				)
			}

			c.workers[configId] = c.queryWorkers[logicalId]
			c.queryWorkersCallbook[logicalId] = client
			//c.queryWorkersDirectory[logicalId] = workerNode.WorkerListenAddr
		} else {
			log.Printf(
				"idx: %v, worker id: %v, adding to query replicas\n",
				idx, workerNode.WorkerConfigId,
			)
			c.queryReplicas[logicalId] = WorkerNode{
				WorkerConfigId:   workerNode.WorkerConfigId,
				WorkerLogicalId:  logicalId,
				WorkerAddr:       workerNode.WorkerAddr,
				WorkerFCheckAddr: workerNode.WorkerFCheckAddr,
				WorkerListenAddr: workerNode.WorkerListenAddr,
				IsReplica:        true,
			}
			c.workers[configId] = c.queryReplicas[logicalId]
			fmt.Printf(
				"Worker %v assigned logical id %v as isReplica %v\n",
				workerNode.WorkerConfigId, logicalId,
				c.queryReplicas[logicalId].IsReplica,
			)
		}

		// assign the same logical id for main & replica
		if idx%2 != 0 {
			logicalId++
		}

		idx++
		if int(logicalId) == workerCount {
			break
		}
	}
}

func (c *Coord) handleFailover(logicalId uint32) {
	log.Printf(
		"BEFORE HANDLEFAILOVER query w: %v, query r: %v, "+
			"logical id: %v\n",
		c.queryWorkers,
		c.queryReplicas, logicalId,
	)
	log.Printf("HandleFailOver - last checkpoint # %v", c.lastCheckpointNumber)
	c.promoteReplicaWorkerToMain(logicalId)
	c.broadcastNewMainWorker(logicalId)
	c.promoteWorkerToReplica(logicalId)
	log.Printf(
		"AFTER HANDLEFAILOVER query w: %v, query r: %v, "+
			"logical id: %v\n",
		c.queryWorkers,
		c.queryReplicas, logicalId,
	)
}

func (c *Coord) promoteReplicaWorkerToMain(logicalId uint32) {
	mainWorker := c.queryReplicas[logicalId]
	mainWorker.IsReplica = false
	c.queryWorkers[logicalId] = mainWorker
	mainClient, err := util.DialRPC(mainWorker.WorkerListenAddr)
	util.CheckErr(err, "handleFailover - failed to dial new main worker node\n")
	c.queryWorkersCallbook[logicalId] = mainClient
	delete(c.queryReplicas, logicalId)
	log.Printf("After - query w: %v, query r: %v\n", c.queryWorkers, c.queryReplicas)
}

func (c *Coord) promoteWorkerToReplica(logicalId uint32) {
	newReplica := c.GetIdleWorker()
	if newReplica != (WorkerNode{}) {
		c.initReplica(&newReplica, logicalId)
		c.assignReplica(newReplica, logicalId)
		c.initReplicaCheckpoints(newReplica, logicalId)
	}
}

func (c *Coord) initReplica(replica *WorkerNode, logicalId uint32) {
	replica.IsReplica = true
	replica.WorkerLogicalId = logicalId
}

func (c *Coord) assignReplica(replica WorkerNode, logicalId uint32) {
	replicaWorker := PromotedWorker{
		LogicalId: logicalId,
		Worker:    replica,
	}
	var result PromotedWorker
	c.queryWorkersCallbook[logicalId].Call(
		"Worker.UpdateReplica", replicaWorker, &result,
	)
}

func (c *Coord) broadcastNewMainWorker(newWorkerLogicalId uint32) {
	newMainWorker := c.queryWorkers[newWorkerLogicalId]
	waitForWorkerCount := len(c.queryWorkers) - 1
	c.workerDoneFailover = make(chan *rpc.Call, waitForWorkerCount)

	for wLogicalId, _ := range c.queryWorkers {
		if wLogicalId != newWorkerLogicalId {
			log.Printf(
				"Coord: handleFailover: calling"+
					" HandleFailover on worker %v\n", wLogicalId,
			)
			promotedWorker := PromotedWorker{
				LogicalId: newWorkerLogicalId,
				Worker:    newMainWorker,
			}
			var result PromotedWorker
			c.queryWorkersCallbook[wLogicalId].Go(
				"Worker.HandleFailover", promotedWorker, &result,
				c.workerDoneFailover,
			)
		}
	}

	go c.blockForWorkerUpdate(waitForWorkerCount, c.workerDoneFailover)
}

func (c *Coord) initReplicaCheckpoints(replica WorkerNode, logicalId uint32) {
	log.Printf("InitReplica - initializing replica (logical id = %v)", logicalId)
	c.queryReplicas[logicalId] = replica

	if c.lastCheckpointNumber < c.checkpointFrequency {
		replicaClient, err := util.DialRPC(replica.WorkerListenAddr)
		if err != nil {
			log.Printf("HandleFailover - failed to contact idle replica worker")
		}

		defer replicaClient.Close()

		startSuperStep := StartSuperStep{
			NumWorkers:            uint8(len(c.queryWorkers)),
			WorkerDirectory:       c.GetWorkerDirectory(),
			WorkerLogicalId:       logicalId,
			HasReplicaInitialized: true,
			Query:                 c.query,
		}

		var superStepReply StartSuperStep
		replicaClient.Call("Worker.StartQuery", startSuperStep, &superStepReply)
	} else {
		mainWorkerClient := c.queryWorkersCallbook[logicalId]
		var unused uint64
		mainWorkerClient.Call("Worker.TransferCheckpointToReplica", c.lastCheckpointNumber, &unused)
	}
}

func (c *Coord) IsActiveWorker(w WorkerNode) bool {
	_, isMain := c.queryWorkers[w.WorkerLogicalId]
	_, isReplica := c.queryReplicas[w.WorkerLogicalId]
	return isMain || isReplica
}

func (c *Coord) getAllWorkersReady() bool {
	c.workerReadyMapMutex.Lock()

	// if all workers are ready, call RevertToLastCheckpoint
	allWorkersReady := true

	for _, ready := range c.workerReadyMap {
		if !ready {
			allWorkersReady = false
			break
		}
	}
	c.workerReadyMapMutex.Unlock()

	return allWorkersReady
}

// this function does what JoinWorker previously did.
//this is because we want to restart a checkpoint not when a failing worker
//rejoins, but when all workers update their callbook in the case of a failure.
// this is the failover handling that *no longer requires* the failing worker to
//rejoin the same query!
func (c *Coord) blockForWorkerUpdate(
	numWorkers int,
	workerDoneFailover chan *rpc.Call,
) {
	readyWorkerCounter := 0

	for {
		select {
		case call := <-workerDoneFailover:
			if call.Error != nil {
				log.Printf(
					"blockForWorkerUpdate - %v: received error: %v\n",
					call.ServiceMethod, call.Error,
				)
			} else {
				if updatedWorker, ok := call.Reply.(*PromotedWorker); ok {
					readyWorkerCounter++
					log.Printf(
						"blockForWorkerUpdate - %v: %d workers are"+
							" ready!\n", call.ServiceMethod, readyWorkerCounter,
					)

					if readyWorkerCounter == numWorkers {
						// when all workers updated their callbook,
						// restart the superstep
						c.workerReadyMapMutex.Lock()
						c.workerReadyMap[updatedWorker.LogicalId] = true
						c.workerReadyMapMutex.Unlock()

						allWorkersReady := c.getAllWorkersReady()

						if allWorkersReady {
							log.Printf(
								"blockForWorkerUpdate: all %v workers"+
									" ready, calling restartCheckpoint!\n",
								len(c.workerReadyMap),
							)
							c.restartCheckpoint()
							c.blockWorkersReady(
								len(c.workerReadyMap),
								c.workerDoneRestart,
							)
						}
					}
				}
			}
		}
	}
}

func (c *Coord) blockWorkersReady(
	numWorkers int, workerDone chan *rpc.Call,
) {
	readyWorkerCounter := 0
	inactiveWorkerCounter := 0
	var computeResult interface{} // result from a single superstep
	superstepMessages := make(VertexMessages)
	workerVertices := make(WorkerVertices)
	var workerVerticesMutex sync.Mutex

	for {
		select {
		case call := <-workerDone:
			if call.Error != nil {
				log.Printf(
					"blockWorkersReady - %v: received error: %v\n",
					call.ServiceMethod, call.Error,
				)
			} else {
				if ssComplete, ok := call.Reply.(*ProgressSuperStepResult); ok {
					if !ssComplete.IsActive {
						inactiveWorkerCounter++
					}
					// set the value returned from the worker
					if ssComplete.CurrentValue != nil {
						computeResult = ssComplete.CurrentValue
					}

					// add worker's vertex messages to the messages collection
					for vId, messages := range ssComplete.Messages {
						superstepMessages[vId] = messages
					}
				}

				if startComplete, ok := call.Reply.(*StartSuperStepResult); ok {
					log.Printf(
						"!!!!!!Coord completed startquery for worker"+
							" %v, vertices: %v\n",
						startComplete.WorkerLogicalId,
						startComplete.Vertices,
					)
					log.Printf(
						"!!!!!!Coord BEFORE workervertices: %v\n",
						workerVertices,
					)
					workerVerticesMutex.Lock()
					// TODO bug: something is wrong with workercallbook & ids
					workerVertices[startComplete.WorkerLogicalId] = startComplete.Vertices
					log.Printf(
						"!!!!!!Coord AFTER workervertices: %v\n",
						workerVertices,
					)
					workerVerticesMutex.Unlock()
				}

				readyWorkerCounter++
				isComputeComplete := inactiveWorkerCounter == numWorkers

				isRestart := false

				// set isRestart to true if this is a recovery superstep
				if _, ok := call.Reply.(*RestartSuperStep); ok {
					isRestart = true
				}

				log.Printf(
					"blockWorkersReady - %v: %d workers ready! %d"+
						" workers inactive!\n",
					call.ServiceMethod,
					readyWorkerCounter,
					inactiveWorkerCounter,
				)

				if readyWorkerCounter == numWorkers {
					c.allWorkersReady <- superstepDone{
						allWorkersInactive: isComputeComplete,
						isSuccess:          true,
						value:              computeResult,
						isRestart:          isRestart,
						messages:           superstepMessages,
						workerVertices:     workerVertices,
					}
					log.Printf(
						"blockWorkersReady - all workers are"+
							" done, sending query value: %v\n",
						computeResult,
					)
					readyWorkerCounter = 0
					inactiveWorkerCounter = 0
					return
				}
			}
		}
	}
}

func (c *Coord) UpdateCheckpoint(
	msg CheckpointMsg, reply *CheckpointMsg,
) error {
	// save the last SuperStep # checkpointed by this worker
	c.mx.Lock()
	defer c.mx.Unlock()
	c.lastWorkerCheckpoints[msg.WorkerId] = msg.SuperStepNumber

	// update global SuperStep # if needed
	allWorkersUpdated := true
	for wId, _ := range c.queryWorkers {
		if c.lastWorkerCheckpoints[wId] != msg.SuperStepNumber {
			allWorkersUpdated = false
			break
		}
	}

	if allWorkersUpdated {
		c.lastCheckpointNumber = msg.SuperStepNumber
		log.Printf(
			"UpdateCheckpoint: coord updated checkpoint number to %v\n"+
				"", c.lastCheckpointNumber,
		)
	}

	*reply = msg
	return nil
}

// TODO we need a framework to invoke a given RPC on all workers and wait for
// them to finish, and reuse that framework. function: pass in the RPC name,
//return the results back to the caller
//func (c *Coord) endQuery(
//	method string, params interface{},
//	workerDoneCh chan *rpc.Call
//) {
//	for _, wClient := range c.queryWorkersCallbook {
//		var result interface{}
//
//		wClient.Go(
//			fmt.Sprintf("Worker.%s", method), params, &result,
//			workerDoneCh)
//	}
//
//	readyWorkerCounter := 0
//	numWorkers := len(c.queryWorkers)
//
//	for {
//		select {
//		case call := <-workerDoneCh:
//			log.Printf("Coord endQuery call done: %v\n", call)
//			//reply := call.Reply
//			readyWorkerCounter++
//			if readyWorkerCounter == numWorkers {
//				log.Printf("Coord endQuery all %v workers finished query\n",
//					readyWorkerCounter)
//				return
//			}
//		}
//	}
//}
func (c *Coord) endQuery(params EndQuery) {
	readyWorkerCounter := 0
	numWorkers := len(c.queryWorkers)
	workerDoneCh := make(chan *rpc.Call, numWorkers)

	for wId, wClient := range c.queryWorkersCallbook {
		var result EndQuery

		wClient.Go(
			fmt.Sprintf("Worker.EndQuery"), params, &result,
			workerDoneCh,
		)
		log.Printf("Coord endQuery: called EndQuery on worker %v\n", wId)
	}

	for {
		select {
		case call := <-workerDoneCh:
			log.Printf("Coord endQuery call done: %v\n", call)
			//reply := call.Reply
			readyWorkerCounter++
			if readyWorkerCounter == numWorkers {
				log.Printf(
					"Coord endQuery all %v workers finished query\n",
					readyWorkerCounter,
				)
				return
			}
		}
	}
}

func (c *Coord) Compute(logger *log.Logger) (interface{}, error) {
	// keep sending messages to workers, until everything has completed
	// need to make it concurrent; so put in separate channel
	numWorkers := len(c.queryWorkers)

	for {
		select {
		case wId := <-c.restartSuperStepCh:
			log.Printf(
				"Compute: received failure of worker"+
					" %v!\n", wId,
			)
			c.workerReadyMapMutex.Lock()
			c.workerReadyMap[wId] = false
			c.workerReadyMapMutex.Unlock()
		case result := <-c.allWorkersReady:
			// if the result is from StartQuery,
			//send the worker vertices to client (
			// notify channel that worker vertices are populated)
			if len(result.workerVertices) > 0 {
				log.Printf(
					"!!!!!!!Coord - created worker vertices: %v\n",
					result.workerVertices,
				)
				c.fetchGraphDone <- result.workerVertices
			}

			// here the messages of for a given superstep for all vertices
			// is collected
			if result.allWorkersInactive {
				log.Printf(
					"Compute: complete with result %v!\n",
					result.value,
				)
				logger.Printf(
					"Completed computation with result %v\n",
					result.value,
				)

				// send ssn to queryProgress channel
				//c.queryProgress <- queryProgress{
				//	superstepNumber: c.
				//		superStepNumber, done: true,
				//	messages: result.messages,
				//}

				// TODO RPC to instruct all workers that the computation
				// finished
				endQuery := EndQuery{}
				go c.endQuery(endQuery)
				log.Printf(
					"Compute: finished endQuery, sending result %v\n",
					result.value,
				)

				if result.value == nil {
					// target vertex does not exist
					return -1, nil
				}
				return result.value, nil
			}
			start := time.Now()

			shouldCheckPoint := c.superStepNumber%c.checkpointFrequency == 0
			// call workers query handler
			progressSuperStep := ProgressSuperStep{
				SuperStepNum: c.superStepNumber,
				IsCheckpoint: shouldCheckPoint,
				IsRestart:    result.isRestart,
			}
			log.Printf(
				"Compute: progressing super step # %d, "+
					"should checkpoint: %v, is restart: %v\n",
				c.superStepNumber, shouldCheckPoint, result.isRestart,
			)

			c.workerDoneCompute = make(chan *rpc.Call, numWorkers)
			for _, worker := range c.queryWorkersCallbook {
				var result ProgressSuperStepResult
				worker.Go(
					"Worker.ComputeVertices", &progressSuperStep, &result,
					c.workerDoneCompute,
				)
			}
			go c.blockWorkersReady(numWorkers, c.workerDoneCompute)

			// send ssn to queryProgress channel
			//c.queryProgress <- queryProgress{
			//	superstepNumber: c.
			//		superStepNumber, done: false,
			//	messages: result.messages,
			//}

			duration := time.Since(start)
			logger.Printf(
				"Compute superstep %v took %v s\n",
				c.superStepNumber, duration.Seconds(),
			)

			c.superStepNumber += 1
			time.Sleep(time.Second * 2)
		}
	}
}

func (c *Coord) restartCheckpoint() {
	log.Printf("restart checkpoint with %v\n", c.lastCheckpointNumber)
	checkpointNumber := c.lastCheckpointNumber
	numWorkers := len(c.queryWorkers)

	restartSuperStep := RestartSuperStep{
		SuperStepNumber: checkpointNumber, NumWorkers: uint8(numWorkers),
		Query: c.query,
	}

	c.workerDoneRestart = make(chan *rpc.Call, numWorkers)
	for wId, wClient := range c.queryWorkersCallbook {
		log.Printf(
			"restart checkpoint: calling"+
				" RevertToLastCheckpoint on worker %v\n", wId,
		)
		var result RestartSuperStep
		wClient.Go(
			"Worker.RevertToLastCheckpoint", restartSuperStep, &result,
			c.workerDoneRestart,
		)
	}
	c.superStepNumber = checkpointNumber
}

// todo: joinworker only adds to c.workers
func (c *Coord) JoinWorker(w WorkerNode, reply *WorkerNode) error {
	log.Printf("JoinWorker: Adding worker %d\n", w.WorkerConfigId)

	_, err := util.DialRPC(w.WorkerListenAddr)
	if err != nil {
		log.Printf(
			"JoinWorker: coord could not dial worker addr %v, err: %v\n",
			w.WorkerListenAddr, err,
		)
		return err
	}

	go c.monitor(w)

	if _, ok := c.workers[w.WorkerConfigId]; ok {
		log.Fatalf(
			"JoinWorker: worker with config id %v already exists in"+
				" workers!\n", w.WorkerConfigId,
		)
	}
	c.workers[w.WorkerConfigId] = w
	fmt.Printf(
		"JoinWorker: added worker %v, workers: %v\n",
		w.WorkerConfigId, c.workers,
	)

	// return nil for no errors
	return nil
}

func listenWorkers(workerAPIListenAddr string) {
	wlisten, err := net.Listen("tcp", workerAPIListenAddr)
	if err != nil {
		log.Printf("listenWorkers: Error listening: %v\n", err)
	}
	log.Printf(
		"listenWorkers: Listening for workers at %v\n",
		workerAPIListenAddr,
	)

	for {
		conn, err := wlisten.Accept()
		if err != nil {
			log.Printf(
				"listenWorkers: Error accepting worker: %v\n", err,
			)
		}
		go rpc.ServeConn(conn) // blocks while serving connection until client hangs up
	}
}

func (c *Coord) monitor(w WorkerNode) {

	// get random port for heartbeats
	//hBeatLocalAddr, _ := net.ResolveUDPAddr("udp", strings.Split(c.WorkerAPIListenAddr, ":")[0]+":0")

	epochNonce := rand.Uint64()

	notifyCh, _, err := fchecker.Start(
		fchecker.StartStruct{
			strings.Split(c.workerAPIListenAddr, ":")[0] + ":0",
			epochNonce,
			strings.Split(c.workerAPIListenAddr, ":")[0] + ":0",
			w.WorkerFCheckAddr,
			c.lostMsgsThresh, w.WorkerConfigId,
		},
	)
	if err != nil || notifyCh == nil {
		log.Printf(
			"monitor: fchecker failed to connect. "+
				"notifyCh nil and/or received err: %v\n",
			err,
		)
	}

	log.Printf("monitor: Fcheck for Worker %d running\n", w.WorkerConfigId)
	for {
		select {
		case notify := <-notifyCh:
			log.Printf(
				"monitor: failedWorker %v failed: %s\n", w.WorkerConfigId,
				notify,
			)
			isRunningQuery := len(c.queryWorkers) > 0
			failedWorker := c.workers[w.WorkerConfigId]
			delete(c.workers, w.WorkerConfigId)

			if isRunningQuery {
				_, isMainWorker := c.queryWorkers[failedWorker.
					WorkerLogicalId]
				_, isReplicaWorker := c.queryReplicas[failedWorker.
					WorkerLogicalId]
				log.Printf(
					"is main: %v, is replica: %v\n", isMainWorker,
					isReplicaWorker,
				)
				if isMainWorker {
					log.Printf("monitor: MAIN WORKER failed\n")
					c.handleFailover(failedWorker.WorkerLogicalId)
				} else if isReplicaWorker {
					log.Printf(
						"monitor: REPLICA WORKER failed\n",
					)
					c.promoteWorkerToReplica(failedWorker.WorkerLogicalId)
				} else {
					log.Printf(
						"monitor: idle worker has failed.")
				}

				c.restartSuperStepCh <- failedWorker.WorkerLogicalId
				return
			}
		}
	}
}

func (c *Coord) AddWorker(context *gin.Context) {
	context.JSON(http.StatusOK, gin.H{"workerId": 1})
}

func (c *Coord) DeleteWorker(context *gin.Context) {
	workerId := context.Param("id")
	context.JSONP(http.StatusOK, gin.H{"workerId": workerId})
}

func (c *Coord) listenExternalRequests(externalAPIListenAddr string) {
	router := gin.Default()
	externalAPI := router.Group("/api")
	{
		externalAPI.POST("/worker", c.AddWorker)
		externalAPI.DELETE("/worker/:id", c.DeleteWorker)
	}
	log.Printf(
		"listenExternalRequests: Listening on %v\n", externalAPIListenAddr,
	)
	if err := router.Run(externalAPIListenAddr); err != nil {
		log.Fatalf("listenExternalRequests: Error while serving : %v", err)
	}
}

func (c *Coord) listenClientsgRPC(clientAPIListenAddr string) {
	lis, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		log.Printf("listenClients: Error listening: %v\n", err)
	}
	log.Printf(
		"listenClients: Listening for clients at %v\n",
		clientAPIListenAddr,
	)

	s := grpc.NewServer()
	coordgRPC.RegisterCoordServer(s, c)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("listenClients: Error while serving : %v", err)
	}
}

func (c *Coord) GetWorkerDirectory() WorkerDirectory {
	directory := make(WorkerDirectory)

	for _, workerNode := range c.queryWorkers {
		directory[workerNode.WorkerLogicalId] = workerNode.WorkerListenAddr
	}

	return directory
}

func (c *Coord) GetIdleWorker() WorkerNode {
	var newReplica WorkerNode
	for _, w := range c.workers {
		if !c.IsActiveWorker(w) {
			newReplica = w
			log.Printf("found idle worker: %v\n", w)
			break
		}
	}
	return newReplica
}

// Only returns when network or other unrecoverable errors occur
func (c *Coord) Start(
	clientAPIListenAddr string, workerAPIListenAddr string,
	externalAPIListenAddr string,
	lostMsgsThresh uint8, checkpointSteps uint64,
) error {

	c.clientAPIListenAddr = clientAPIListenAddr
	c.workerAPIListenAddr = workerAPIListenAddr
	c.lostMsgsThresh = lostMsgsThresh
	c.checkpointFrequency = checkpointSteps

	err := rpc.Register(c)
	log.Printf("error: %v\n", err)
	util.CheckErr(err, "Coord could not register RPCs")

	wg := sync.WaitGroup{}
	wg.Add(coordProcesses)
	go listenWorkers(workerAPIListenAddr)
	go c.listenClientsgRPC(clientAPIListenAddr)
	go c.listenExternalRequests(externalAPIListenAddr)
	wg.Wait()

	// will never return
	return nil
}
