package bagel

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"project/database"
	fchecker "project/fcheck"
	"project/util"
	"strings"
	"sync"
)

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

func NewCoord() *Coord {
	return &Coord{
		clientAPIListenAddr:   "",
		workerAPIListenAddr:   "",
		lostMsgsThresh:        0,
		lastWorkerCheckpoints: make(map[uint32]uint64),
		workers:               make(map[uint32]*rpc.Client),
		queryWorkers:          make(WorkerCallBook),
		queryWorkersDirectory: make(WorkerDirectory),
		superStepNumber:       1,
	}
}

// this is the start of the query where coord notifies workers to initialize
// state for SuperStep 0
func (c *Coord) StartQuery(q Query, reply *QueryResult) error {
	log.Printf("StartQuery: received query: %v\n", q)

	if len(c.workers) == 0 {
		log.Printf("StartQuery: No workers available - will block until workers join\n")
	}

	for len(c.workers) == 0 {
		// block while no workers available
	}

	// validate vertices sent by the client query
	for _, vId := range q.Nodes {
		_, err := database.GetVertexById(int(vId))
		if err != nil {
			reply.Error = err.Error()
			return nil
		}
	}

	// go doesn't have a deep copy method :(
	c.queryWorkers = make(map[uint32]*rpc.Client)
	for k, v := range c.workers {
		c.queryWorkers[k] = v
	}

	// initialize workerReady map
	c.workerReadyMap = make(map[uint32]bool)
	for k, _ := range c.queryWorkers {
		c.workerReadyMap[k] = true
	}

	// create new map of checkpoints for a new query which may have different number of workers
	c.lastCheckpointNumber = 0
	c.lastWorkerCheckpoints = make(map[uint32]uint64)
	c.superStepNumber = 1

	startSuperStep := StartSuperStep{
		NumWorkers:      uint8(len(c.queryWorkers)),
		WorkerDirectory: c.queryWorkersDirectory,
		Query:           q,
	}

	numWorkers := len(c.queryWorkers)
	c.workerDoneStart = make(chan *rpc.Call, numWorkers)
	c.workerDoneCompute = make(chan *rpc.Call, numWorkers)
	c.workerDoneRestart = make(chan *rpc.Call, numWorkers)
	c.allWorkersReady = make(chan superstepDone, 1)
	c.restartSuperStepCh = make(chan uint32, numWorkers)

	c.query = q

	log.Printf(
		"StartQuery: computing query %v with %d workers ready!\n", q,
		numWorkers,
	)

	// call workers start query handlers
	for _, wClient := range c.queryWorkers {
		var result interface{}
		wClient.Go(
			"Worker.StartQuery", startSuperStep, &result,
			c.workerDoneStart,
		)
	}

	go c.blockWorkersReady(len(c.queryWorkers), c.workerDoneStart)

	// start query computation
	result, err := c.Compute()
	if err != nil {
		log.Printf("StartQuery: Compute returned err: %v", err)
	}

	reply.Query = q
	reply.Result = result

	c.queryWorkers = nil
	c.query = Query{}

	// return nil for no errors
	return nil
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
			log.Printf(
				"blockWorkersReady - %v: received reply: %v\n",
				call.ServiceMethod, call.Reply,
			)
			log.Printf(
				"blockworkersready - %v: readyworkercounter: %v\n",
				call.ServiceMethod, readyWorkerCounter,
			)

			if call.Error != nil {
				log.Printf(
					"blockWorkersReady - %v: received error: %v\n",
					call.ServiceMethod, call.Error,
				)
			} else {

				if ssComplete, ok := call.Reply.(*ProgressSuperStepResult); ok {
					if !ssComplete.IsActive {
						inactiveWorkerCounter++
						log.Printf(
							"Worker reported as being active = %v\n",
							ssComplete.IsActive,
						)
					}
					// set the value returned from the worker
					if ssComplete.CurrentValue != nil {
						computeResult = ssComplete.CurrentValue
					}
					log.Printf(
						"blockWorkersReady - current query value: %v\n",
						computeResult,
					)
				}

				readyWorkerCounter++
				isComputeComplete := inactiveWorkerCounter == numWorkers

				isRestart := false

				// set isRestart to true if this is a recovery superstep
				if _, ok := call.Reply.(*RestartSuperStep); ok {
					isRestart = true
				}

				log.Printf(
					"blockWorkersReady - %v: %d workers ready! %d workers inactive!\n",
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
						"blockWorkersReady - sending query value: %v\n",
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
	}

	*reply = msg
	return nil
}

func (c *Coord) Compute() (interface{}, error) {
	// keep sending messages to workers, until everything has completed
	// need to make it concurrent; so put in separate channel

	numWorkers := len(c.queryWorkers)

	for {
		select {
		case wId := <-c.restartSuperStepCh:
			log.Printf("Coord - compute received failure of worker %v!\n", wId)
			c.workerReadyMapMutex.Lock()
			c.workerReadyMap[wId] = false
			c.workerReadyMapMutex.Unlock()
		case result := <-c.allWorkersReady:
			log.Printf(
				"Coord-running compute with superstep: %v, isrestart: %v\n",
				c.superStepNumber, result.isRestart,
			)

			if result.allWorkersInactive {
				log.Printf(
					"Computation is complete with result %v\n!", result.value,
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
				"Coord: Compute: progressing super step # %d, should checkpoint %v \n",
				c.superStepNumber, shouldCheckPoint,
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

func (c *Coord) restartCheckpoint() {
	log.Printf("Coord - restart checkpoint with %v\n", c.lastCheckpointNumber)
	checkpointNumber := c.lastCheckpointNumber
	numWorkers := len(c.queryWorkers)

	restartSuperStep := RestartSuperStep{
		SuperStepNumber: checkpointNumber, NumWorkers: uint8(numWorkers),
		Query: c.query, WorkerDirectory: c.queryWorkersDirectory,
	}

	c.workerDoneRestart = make(chan *rpc.Call, numWorkers)
	for wId, wClient := range c.queryWorkers {
		log.Printf("Coord - calling RevertToLastCheckpoint on worker %v\n", wId)
		var result RestartSuperStep
		wClient.Go(
			"Worker.RevertToLastCheckpoint", restartSuperStep, &result,
			c.workerDoneRestart,
		)
	}
	c.superStepNumber = checkpointNumber
}

func (c *Coord) JoinWorker(w WorkerNode, reply *WorkerNode) error {
	log.Printf("JoinWorker: Adding worker %d\n", w.WorkerId)

	client, err := util.DialRPC(w.WorkerListenAddr)
	if err != nil {
		log.Printf(
			"JoinWorker: coord could not dial worker addr %v, err: %v\n",
			w.WorkerListenAddr, err,
		)
		return err
	}

	c.queryWorkersDirectory[w.WorkerId] = w.WorkerListenAddr

	go c.monitor(w)

	if _, ok := c.queryWorkers[w.WorkerId]; ok {
		// joining worker is restarted process of failed worker used in current query
		log.Printf(
			"JoinWorker: Worker %d rejoined after failure\n",
			w.WorkerId,
		)
		c.queryWorkers[w.WorkerId] = client
		c.workers[w.WorkerId] = client

		c.workerReadyMapMutex.Lock()
		c.workerReadyMap[w.WorkerId] = true

		// if all workers are ready, call RevertToLastCheckpoint
		allWorkersReady := true

		for _, ready := range c.workerReadyMap {
			if !ready {
				allWorkersReady = false
				break
			}
		}
		c.workerReadyMapMutex.Unlock()

		if allWorkersReady {
			log.Printf(
				"Coord - Join Worker: all %v workers ready, "+
					"calling restartCheckpoint!\n", len(c.workerReadyMap),
			)
			c.restartCheckpoint()
			c.blockWorkersReady(len(c.workerReadyMap), c.workerDoneRestart)
		}
	} else {
		c.workers[w.WorkerId] = client
		log.Printf(
			"JoinWorker: New Worker %d successfully added. %d Workers joined\n",
			w.WorkerId, len(c.workers),
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
		log.Printf("listenWorkers: accepted connection to worker\n")
		go rpc.ServeConn(conn) // blocks while serving connection until client hangs up
	}
}

func (c *Coord) monitor(w WorkerNode) {

	// get random port for heartbeats
	//hBeatLocalAddr, _ := net.ResolveUDPAddr("udp", strings.Split(c.WorkerAPIListenAddr, ":")[0]+":0")
	log.Printf(
		"monitor: Starting fchecker for Worker %d at %v\n", w.WorkerId,
		w.WorkerAddr,
	)

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
			"monitor: fchecker failed to connect. notifyCh nil and/or received err: %v\n",
			err,
		)
	}

	log.Printf("monitor: Fcheck for Worker %d running\n", w.WorkerId)
	for {
		select {
		case notify := <-notifyCh:
			log.Printf("monitor: worker %v failed: %s\n", w.WorkerId, notify)
			if len(c.queryWorkers) > 0 {
				c.restartSuperStepCh <- w.WorkerId
				return
			}
		}
	}
}

func listenClients(clientAPIListenAddr string) {

	wlisten, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		log.Printf("listenClients: Error listening: %v\n", err)
	}
	log.Printf(
		"listenClients: Listening for clients at %v\n",
		clientAPIListenAddr,
	)

	for {
		conn, err := wlisten.Accept()
		if err != nil {
			log.Printf(
				"listenClients: Error accepting client: %v\n", err,
			)
		}
		log.Printf("listenClients: Accepted connection to client\n")
		go rpc.ServeConn(conn) // blocks while serving connection until client hangs up
	}
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
	util.CheckErr(err, "Coord could not register RPCs")
	log.Printf("Start: accepting RPCs from workers and clients\n")

	wg := sync.WaitGroup{}
	wg.Add(2)
	go listenWorkers(workerAPIListenAddr)
	go listenClients(clientAPIListenAddr)
	wg.Wait()

	// will never return
	return nil
}
