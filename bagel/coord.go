package bagel

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	coordgRPC "project/bagel/proto/coord"
	"project/database"
	fchecker "project/fcheck"
	"project/util"
	"strings"
	"sync"
	"time"

	//"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/grpc"
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
	svc := database.GetDynamoClient()
	for _, vId := range q.Nodes {
		// TODO: include db name as part of query
		_, err := database.GetVertexByID(svc, int64(vId), q.TableName)
		if err != nil {
			reply.Error = err.Error()
			return &reply, nil
		}
	}

	// go doesn't have a deep copy method :(
	//c.queryWorkers = make(map[uint32]*rpc.Client)
	//for k, v := range c.workers {
	//	c.queryWorkers[k] = v
	//}

	//c.queryWorkersFailover = make(map[uint32]FailoverQueryWorker)
	//for k, v := range c.workers {
	//	// todo
	//}
	// todo replace with workerCount from client
	testWorkerCount := 3
	c.assignQueryWorkers(testWorkerCount)

	// initialize workerReady map
	// todo: populate failover query workers with main/replica
	c.workerReadyMap = make(map[uint32]bool)
	//for k, _ := range c.queryWorkers {
	//	c.workerReadyMap[k] = true
	//}
	for logicalId := range c.queryWorkers {
		c.workerReadyMap[logicalId] = true
	}

	fmt.Printf("query workers: %v\n", c.queryWorkers)
	fmt.Printf("query replicas: %v\n", c.queryReplicas)

	//return nil, nil
	//return &reply, nil

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
		NumWorkers:      uint8(len(c.queryWorkers)),
		WorkerDirectory: c.GetWorkerDirectory(),
		Query:           coordQuery,
	}

	numWorkers := len(c.queryWorkers)
	c.workerDoneStart = make(chan *rpc.Call, numWorkers)
	c.workerDoneCompute = make(chan *rpc.Call, numWorkers)
	c.workerDoneRestart = make(chan *rpc.Call, numWorkers)
	c.workerDoneFailover = make(chan *rpc.Call, numWorkers)
	c.allWorkersReady = make(chan superstepDone, 1)
	c.restartSuperStepCh = make(chan uint32, numWorkers)

	c.query = coordQuery

	log.Printf(
		"StartQuery: computing query %v with %d workers ready!\n", q,
		numWorkers,
	)

	// call workers start query handlers
	for logicalId, client := range c.queryWorkersCallbook {
		var result interface{}
		startSuperStep.WorkerLogicalId = logicalId
		startSuperStep.ReplicaAddr = c.queryReplicas[logicalId].WorkerListenAddr
		client.Go(
			"Worker.StartQuery", startSuperStep, &result,
			c.workerDoneStart,
		)
	}

	go c.blockWorkersReady(
		len(c.queryWorkers),
		c.workerDoneStart,
	)

	// create a log file
	logFile, err := os.OpenFile(
		"coord.log", os.O_WRONLY|os.O_CREATE, 0644,
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

/* end of proto config */

type CoordConfig struct {
	ClientAPIListenAddr     string // client will know this and use it to contact coord
	WorkerAPIListenAddr     string // new joining workers will message this addr
	LostMsgsThresh          uint8  // fcheck
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
}

type superstepDone struct {
	allWorkersInactive bool
	isSuccess          bool
	value              interface{}
	isRestart          bool
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

	logicalId := uint32(0)
	idx := 0
	for _, workerNode := range c.workers {
		if idx%2 == 0 {
			c.queryWorkers[logicalId] = WorkerNode{
				WorkerConfigId:   workerNode.WorkerConfigId,
				WorkerLogicalId:  logicalId,
				WorkerAddr:       workerNode.WorkerAddr,
				WorkerFCheckAddr: workerNode.WorkerFCheckAddr,
				WorkerListenAddr: workerNode.WorkerListenAddr,
				IsReplica:        false,
				//Client:           workerNode.Client,
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

			c.queryWorkersCallbook[logicalId] = client
			//c.queryWorkersDirectory[logicalId] = workerNode.WorkerListenAddr
		} else {
			c.queryReplicas[logicalId] = WorkerNode{
				WorkerConfigId:   workerNode.WorkerConfigId,
				WorkerLogicalId:  logicalId,
				WorkerAddr:       workerNode.WorkerAddr,
				WorkerFCheckAddr: workerNode.WorkerFCheckAddr,
				WorkerListenAddr: workerNode.WorkerListenAddr,
				IsReplica:        true,
				//Client:           workerNode.Client,
			}
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
	mainWorker := c.queryReplicas[logicalId]
	c.queryWorkers[logicalId] = mainWorker
	//c.queryWorkersDirectory[logicalId] = mainWorker.WorkerListenAddr
	delete(c.queryReplicas, logicalId)

	var newReplica WorkerNode
	for _, w := range c.workers {
		if !c.IsActiveWorker(w) {
			newReplica = w
			break
		}
	}

	if newReplica != (WorkerNode{}) {
		c.queryReplicas[logicalId] = newReplica
		// todo
	} else {
		// logging
		log.Printf("Unable to assign replicas - not enough workers\n")
	}

	// broadcast main worker to other workers
	c.workerDoneFailover = make(chan *rpc.Call, len(c.queryWorkers))
	for wLogicalId, workerNode := range c.queryWorkers {
		if wLogicalId != logicalId {
			log.Printf(
				"Coord: handleFailover: calling"+
					" HandleFailover on worker %v\n", wLogicalId,
			)
			promotedWorker := PromotedWorker{
				LogicalId: logicalId,
				Worker:    mainWorker,
			}
			var result PromotedWorker
			c.queryWorkersCallbook[wLogicalId].Go(
				"Worker.HandleFailover", promotedWorker, &result,
				c.workerDoneFailover,
			)

		} else {
			replicaWorker := PromotedWorker{
				LogicalId: logicalId,
				Worker:    newReplica,
			}
			var result PromotedWorker
			c.queryWorkersCallbook[workerNode.WorkerLogicalId].Call(
				"Worker.UpdateReplica", replicaWorker, &result,
			)
		}
	}
	go c.blockForWorkerUpdate(len(c.queryWorkers)-1, c.workerDoneFailover)
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
			if result.allWorkersInactive {
				log.Printf(
					"Compute: complete with result %v!\n",
					result.value,
				)
				logger.Printf(
					"Completed computation with result %v\n",
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

			duration := time.Since(start)
			logger.Printf(
				"Compute superstep %v took %v s\n",
				c.superStepNumber, duration.Seconds(),
			)

			c.superStepNumber += 1
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

	//w.Client = client
	c.workers[w.WorkerConfigId] = w
	fmt.Printf("JoinWorker: workers: %v\n", c.workers)

	log.Printf(
		"JoinWorker: New Worker %d successfully added. "+
			"%d Workers joined\n",
		w.WorkerConfigId, len(c.workers),
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
				"monitor: worker %v failed: %s\n", w.WorkerConfigId,
				notify,
			)
			if len(c.queryWorkers) > 0 {
				worker := c.workers[w.WorkerConfigId]

				if _, ok := c.queryWorkers[worker.
					WorkerLogicalId]; ok {
					c.handleFailover(w.WorkerConfigId)
					c.blockWorkersReady(
						len(c.queryWorkers),
						c.workerDoneFailover,
					)
				} else if _, ok := c.queryReplicas[worker.
					WorkerLogicalId]; ok {
					// try to find new replica...
				} else {
					// return
				}

				c.restartSuperStepCh <- worker.WorkerLogicalId

				return
			}
		}
	}
}

//func listenClients(clientAPIListenAddr string) {
//
//	wlisten, err := net.Listen("tcp", clientAPIListenAddr)
//	if err != nil {
//		log.Printf("listenClients: Error listening: %v\n", err)
//	}
//	log.Printf(
//		"listenClients: Listening for clients at %v\n",
//		clientAPIListenAddr,
//	)
//
//	for {
//		conn, err := wlisten.Accept()
//		if err != nil {
//			log.Printf(
//				"listenClients: Error accepting client: %v\n", err,
//			)
//		}
//		go rpc.ServeConn(conn) // blocks while serving connection until client hangs up
//	}
//}

func (c *Coord) listenClientsgRPC(clientAPIListenAddr string) {
	lis, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		log.Printf("listenClients: Error listening: %v\n", err)
	}
	log.Printf(
		"listenClients: Listening for clients at %v\n",
		clientAPIListenAddr,
	)

	//for {
	//
	//}
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

// Only returns when network or other unrecoverable errors occur
func (c *Coord) Start(
	clientAPIListenAddr string, workerAPIListenAddr string,
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
	wg.Add(2)
	go listenWorkers(workerAPIListenAddr)
	go c.listenClientsgRPC(clientAPIListenAddr)
	wg.Wait()

	// will never return
	return nil
}
