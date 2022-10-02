package bagel

import (
	"context"
	"log"
	"net"
	"net/rpc"
	coordgRPC "project/bagel/proto/coord"
	"project/database"
	"project/util"
	"sync"

	//"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/grpc"
)

/* proto config */
type CoordgRPCServer struct {
	coordgRPC.UnimplementedCoordServer
	coord Coord
}

func (c *CoordgRPCServer) StartQuery(ctx context.Context, q *coordgRPC.Query) (
	*coordgRPC.QueryResult,
	error,
) {
	var reply coordgRPC.QueryResult

	log.Printf("StartQuery: received query: %v\n", q)

	if len(c.coord.workers) == 0 {
		log.Printf(
			"StartQuery: No workers available - will block" +
				" until workers join\n",
		)
	}

	//for len(c.coord.workers) == 0 {
	//	// block while no workers available
	//}

	// validate vertices sent by the client query
	for _, vId := range q.Nodes {
		_, err := database.GetVertexById(int(vId))
		if err != nil {
			reply.Error = err.Error()
			return &reply, nil
		}
	}

	// go doesn't have a deep copy method :(
	c.coord.queryWorkers = make(map[uint32]*rpc.Client)
	for k, v := range c.coord.workers {
		c.coord.queryWorkers[k] = v
	}

	// initialize workerReady map
	c.coord.workerReadyMap = make(map[uint32]bool)
	for k, _ := range c.coord.queryWorkers {
		c.coord.workerReadyMap[k] = true
	}

	// create new map of checkpoints for a new query which may have different number of workers
	c.coord.lastCheckpointNumber = 0
	c.coord.lastWorkerCheckpoints = make(map[uint32]uint64)
	c.coord.superStepNumber = 1

	coordQuery := Query{
		ClientId:  q.ClientId,
		QueryType: q.QueryType,
		Nodes:     q.Nodes,
		Graph:     q.Graph,
	}

	//startSuperStep := StartSuperStep{
	//	NumWorkers:      uint8(len(c.coord.queryWorkers)),
	//	WorkerDirectory: c.coord.queryWorkersDirectory,
	//	Query:           coordQuery,
	//}

	numWorkers := len(c.coord.queryWorkers)
	c.coord.workerDoneStart = make(chan *rpc.Call, numWorkers)
	c.coord.workerDoneCompute = make(chan *rpc.Call, numWorkers)
	c.coord.workerDoneRestart = make(chan *rpc.Call, numWorkers)
	c.coord.allWorkersReady = make(chan superstepDone, 1)
	c.coord.restartSuperStepCh = make(chan uint32, numWorkers)

	c.coord.query = coordQuery

	log.Printf(
		"StartQuery: computing query %v with %d workers ready!\n", q,
		numWorkers,
	)

	log.Printf("is coord server defined? %v\n", c.coord)

	// call workers start query handlers
	//for _, wClient := range c.coord.queryWorkers {
	//	var result interface{}
	//	wClient.Go(
	//		"Worker.StartQuery", startSuperStep, &result,
	//		c.coord.workerDoneStart,
	//	)
	//}
	//
	//go c.coord.blockWorkersReady(
	//	len(c.coord.queryWorkers),
	//	c.coord.workerDoneStart)
	//
	//// start query computation
	//result, err := c.coord.Compute()
	//if err != nil {
	//	log.Printf("StartQuery: Compute returned error: %v\n", err)
	//}
	//log.Printf("StartQuert: computed result: %v\n", result)

	reply.Query = q
	//reply.Result = result
	reply.Result = nil // TODO: fix

	c.coord.queryWorkers = nil
	c.coord.query = Query{}

	// return nil for no errors
	return &reply, nil
	// test
	//log.Printf("is coord server defined? %v\n", c.coord)
	//return &coordgRPC.QueryResult{
	//	Query:  q,
	//	Result: nil,
	//	Error:  "no error!!",
	//}, nil
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
	clientAPIListenAddr   string
	workerAPIListenAddr   string
	lostMsgsThresh        uint8
	workers               WorkerCallBook // worker id --> worker connection
	queryWorkers          WorkerCallBook // workers in use for current query - will be updated at start of query
	queryWorkersDirectory WorkerDirectory
	lastCheckpointNumber  uint64
	lastWorkerCheckpoints map[uint32]uint64
	checkpointFrequency   uint64
	superStepNumber       uint64
	workerDoneStart       chan *rpc.Call // done messages for Worker.StartQuery RPC
	workerDoneCompute     chan *rpc.Call // done messages for Worker.ComputeVertices RPC
	workerDoneRestart     chan *rpc.Call // done messages for Worker.RevertToLastCheckpoint RPC
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

//func NewCoord() *Coord {
//
//	return &Coord{
//		clientAPIListenAddr:   "",
//		workerAPIListenAddr:   "",
//		lostMsgsThresh:        0,
//		lastWorkerCheckpoints: make(map[uint32]uint64),
//		workers:               make(map[uint32]*rpc.Client),
//		queryWorkers:          make(WorkerCallBook),
//		queryWorkersDirectory: make(WorkerDirectory),
//		superStepNumber:       1,
//	}
//}
func NewCoord() *CoordgRPCServer {

	return &CoordgRPCServer{
		UnimplementedCoordServer: coordgRPC.UnimplementedCoordServer{},
		coord: Coord{
			clientAPIListenAddr:   "",
			workerAPIListenAddr:   "",
			lostMsgsThresh:        0,
			lastWorkerCheckpoints: make(map[uint32]uint64),
			workers:               make(map[uint32]*rpc.Client),
			queryWorkers:          make(WorkerCallBook),
			queryWorkersDirectory: make(WorkerDirectory),
			superStepNumber:       1,
		},
	}

	//return &CoordgRPCServer{
	//
	//	coord.clientAPIListenAddr:   "",
	//
	//}
}

// this is the start of the query where coord notifies workers to initialize
// state for SuperStep 0
//func (c *Coord) StartQuery(q Query, reply *QueryResult) error {
//	log.Printf("StartQuery: received query: %v\n", q)
//
//	if len(c.workers) == 0 {
//		log.Printf(
//			"StartQuery: No workers available - will block" +
//				" until workers join\n",
//		)
//	}
//
//	for len(c.workers) == 0 {
//		// block while no workers available
//	}
//
//	// validate vertices sent by the client query
//	for _, vId := range q.Nodes {
//		_, err := database.GetVertexById(int(vId))
//		if err != nil {
//			reply.Error = err.Error()
//			return nil
//		}
//	}
//
//	// go doesn't have a deep copy method :(
//	c.queryWorkers = make(map[uint32]*rpc.Client)
//	for k, v := range c.workers {
//		c.queryWorkers[k] = v
//	}
//
//	// initialize workerReady map
//	c.workerReadyMap = make(map[uint32]bool)
//	for k, _ := range c.queryWorkers {
//		c.workerReadyMap[k] = true
//	}
//
//	// create new map of checkpoints for a new query which may have different number of workers
//	c.lastCheckpointNumber = 0
//	c.lastWorkerCheckpoints = make(map[uint32]uint64)
//	c.superStepNumber = 1
//
//	startSuperStep := StartSuperStep{
//		NumWorkers:      uint8(len(c.queryWorkers)),
//		WorkerDirectory: c.queryWorkersDirectory,
//		Query:           q,
//	}
//
//	numWorkers := len(c.queryWorkers)
//	c.workerDoneStart = make(chan *rpc.Call, numWorkers)
//	c.workerDoneCompute = make(chan *rpc.Call, numWorkers)
//	c.workerDoneRestart = make(chan *rpc.Call, numWorkers)
//	c.allWorkersReady = make(chan superstepDone, 1)
//	c.restartSuperStepCh = make(chan uint32, numWorkers)
//
//	c.query = q
//
//	log.Printf(
//		"StartQuery: computing query %v with %d workers ready!\n", q,
//		numWorkers,
//	)
//
//	// call workers start query handlers
//	for _, wClient := range c.queryWorkers {
//		var result interface{}
//		wClient.Go(
//			"Worker.StartQuery", startSuperStep, &result,
//			c.workerDoneStart,
//		)
//	}
//
//	go c.blockWorkersReady(len(c.queryWorkers), c.workerDoneStart)
//
//	// start query computation
//	result, err := c.Compute()
//	if err != nil {
//		log.Printf("StartQuery: Compute returned error: %v", err)
//	}
//
//	reply.Query = q
//	reply.Result = result
//
//	c.queryWorkers = nil
//	c.query = Query{}
//
//	// return nil for no errors
//	return nil
//}

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

/*
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
	}

	*reply = msg
	return nil
}

*/

/*
func (c *Coord) Compute() (interface{}, error) {
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
				return result.value, nil
			}

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
			for _, wClient := range c.queryWorkers {
				var result ProgressSuperStepResult
				wClient.Go(
					"Worker.ComputeVertices", &progressSuperStep, &result,
					c.workerDoneCompute,
				)
			}
			go c.blockWorkersReady(numWorkers, c.workerDoneCompute)
			c.superStepNumber += 1
		}
	}
}

*/

func (c *Coord) restartCheckpoint() {
	log.Printf("restart checkpoint with %v\n", c.lastCheckpointNumber)
	checkpointNumber := c.lastCheckpointNumber
	numWorkers := len(c.queryWorkers)

	restartSuperStep := RestartSuperStep{
		SuperStepNumber: checkpointNumber, NumWorkers: uint8(numWorkers),
		Query: c.query, WorkerDirectory: c.queryWorkersDirectory,
	}

	c.workerDoneRestart = make(chan *rpc.Call, numWorkers)
	for wId, wClient := range c.queryWorkers {
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

func (c *CoordgRPCServer) JoinWorker(w WorkerNode, reply *WorkerNode) error {
	log.Printf("JoinWorker: Adding worker %d\n", w.WorkerId)

	client, err := util.DialRPC(w.WorkerListenAddr)
	if err != nil {
		log.Printf(
			"JoinWorker: coord could not dial worker addr %v, err: %v\n",
			w.WorkerListenAddr, err,
		)
		return err
	}

	c.coord.queryWorkersDirectory[w.WorkerId] = w.WorkerListenAddr

	//go c.monitor(w)

	log.Printf(
		"JoinWorker: New Worker %d successfully added. "+
			"%d Workers joined\n",
		w.WorkerId, len(c.coord.workers),
	)

	if _, ok := c.coord.queryWorkers[w.WorkerId]; ok {
		// joining worker is restarted process of failed worker used in current query
		log.Printf(
			"JoinWorker: Worker %d added to the current query!\n",
			w.WorkerId,
		)
		c.coord.queryWorkers[w.WorkerId] = client
		c.coord.workers[w.WorkerId] = client

		c.coord.workerReadyMapMutex.Lock()
		c.coord.workerReadyMap[w.WorkerId] = true

		// if all workers are ready, call RevertToLastCheckpoint
		allWorkersReady := true

		for _, ready := range c.coord.workerReadyMap {
			if !ready {
				allWorkersReady = false
				break
			}
		}
		c.coord.workerReadyMapMutex.Unlock()

		if allWorkersReady {
			log.Printf(
				"Join Worker: all %v workers ready, "+
					"calling restartCheckpoint!\n", len(c.coord.workerReadyMap),
			)
			c.coord.restartCheckpoint() // does this need to be c.
			// restartCheckpoint for data to be persisted in c?
			c.coord.blockWorkersReady(
				len(c.coord.workerReadyMap),
				c.coord.workerDoneRestart,
			)
		}
	} else {
		c.coord.workers[w.WorkerId] = client
		log.Printf(
			"JoinWorker: Worker %d will be added in the next query!\n",
			w.WorkerId,
		)
	}

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

/*
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
			c.lostMsgsThresh, w.WorkerId,
		},
	)
	if err != nil || notifyCh == nil {
		log.Printf(
			"monitor: fchecker failed to connect. "+
				"notifyCh nil and/or received err: %v\n",
			err,
		)
	}

	log.Printf("monitor: Fcheck for Worker %d running\n", w.WorkerId)
	for {
		select {
		case notify := <-notifyCh:
			log.Printf(
				"monitor: worker %v failed: %s\n", w.WorkerId,
				notify,
			)
			if len(c.queryWorkers) > 0 {
				c.restartSuperStepCh <- w.WorkerId
				return
			}
		}
	}
}
*/

//func handleShortestPath(w http.ResponseWriter, r *http.Request) {
//	log.Printf(
//		"coord handled shortest path",
//	)
//	fmt.Fprintf(w, "Welcome to shortest path endpoint!")
//}
//
//func listenClientsREST(clientAPIListenAddr string) {
//	router := mux.NewRouter()
//	router.HandleFunc("/shortestpath", handleShortestPath)
//	log.Fatal(http.ListenAndServe(clientAPIListenAddr, router))
//}

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

func listenClientsgRPC(clientAPIListenAddr string) {
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
	coordgRPC.RegisterCoordServer(s, &CoordgRPCServer{})
	//coord.RegisterCoordServer(s, &Coord{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("listenClients: Error while serving : %v", err)
	}

}

// Only returns when network or other unrecoverable errors occur
func (c *CoordgRPCServer) Start(
	clientAPIListenAddr string, workerAPIListenAddr string,
	lostMsgsThresh uint8, checkpointSteps uint64,
) error {

	c.coord.clientAPIListenAddr = clientAPIListenAddr
	c.coord.workerAPIListenAddr = workerAPIListenAddr
	c.coord.lostMsgsThresh = lostMsgsThresh
	c.coord.checkpointFrequency = checkpointSteps

	err := rpc.Register(c)
	log.Printf("error: %v\n", err)
	util.CheckErr(err, "Coord could not register RPCs")

	wg := sync.WaitGroup{}
	wg.Add(2)
	go listenWorkers(workerAPIListenAddr)
	//go listenClients(clientAPIListenAddr)
	//go listenClientsREST(clientAPIListenAddr)
	go listenClientsgRPC(clientAPIListenAddr)
	wg.Wait()

	// will never return
	return nil
}
