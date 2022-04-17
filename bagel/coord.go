package bagel

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	fchecker "project/fcheck"
	"project/util"
	"strings"
	"sync"
	"time"
)

type CoordConfig struct {
	ClientAPIListenAddr     string // client will know this and use it to contact coord
	WorkerAPIListenAddr     string // new joining workers will message this addr
	LostMsgsThresh          uint8  // fcheck
	StepsBetweenCheckpoints uint64
}

type SuperStepDone struct {
	messagesSent        uint64
	allVerticesInactive bool
}

type Coord struct {
	// Coord state may go here
	clientAPIListenAddr   string
	workerAPIListenAddr   string
	lostMsgsThresh        uint8
	workers               WorkerCallBook // worker id --> worker connection
	queryWorkers          WorkerCallBook // workers in use for current query - will be updated at start of query
	queryWorkersDirectory WorkerDirectory
	workersMutex          sync.Mutex
	lastCheckpointNumber  uint64
	lastWorkerCheckpoints map[uint32]uint64
	readyWorkerCounter    int
	failedWorkerCounter   int
	workerCounterMutex    sync.Mutex
	checkpointFrequency   uint64
	superStepNumber       uint64
	workerDoneStart       chan *rpc.Call // done messages for Worker.StartQuery RPC
	workerDoneCompute     chan *rpc.Call // done messages for Worker.ComputeVertices RPC
	workerDoneRestart     chan *rpc.Call // done messages for Worker.RevertToLastCheckpoint RPC
	allWorkersReady       chan superstepDone
	restartSuperStepCh    chan bool
}

// todo: use in allWorkersReady channel (rather than just bool)?
type superstepDone struct {
	allWorkersInactive bool
	isSuccess          bool
	ShortestPathResult int
	PageRankResult     int // todo not sure what the result will be?
}

/*
workerDone chan *rpc.Call
resetWorkerCounter chan bool --> used by fcheck
*/

func NewCoord() *Coord {
	return &Coord{
		clientAPIListenAddr:   "",
		workerAPIListenAddr:   "",
		lostMsgsThresh:        0,
		lastWorkerCheckpoints: make(map[uint32]uint64),
		workers:               make(map[uint32]*rpc.Client),
		queryWorkers:          make(WorkerCallBook),
		queryWorkersDirectory: make(WorkerDirectory),
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

	// go doesn't have a deep copy method :(
	c.queryWorkers = make(map[uint32]*rpc.Client)
	for k, v := range c.workers {
		c.queryWorkers[k] = v
	}

	// create new map of checkpoints for a new query which may have different number of workers
	c.lastWorkerCheckpoints = make(map[uint32]uint64)

	// call workers query handler
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

	log.Printf("StartQuery: computing query %v with %d workers ready!\n", q, numWorkers)

	for _, wClient := range c.queryWorkers {
		var result interface{}
		wClient.Go(
			"Worker.StartQuery", startSuperStep, &result,
			c.workerDoneStart,
		)
	}

	result, err := c.Compute()
	if err != nil {
		log.Printf("StartQuery: Compute returned err: %v", err)
	}

	reply.Query = q
	reply.Result = result

	c.queryWorkers = nil

	// return nil for no errors
	return nil
}

func (c *Coord) blockWorkersReady(
	numWorkers int, workerDone chan *rpc.Call) {
	readyWorkerCounter := 0
	inactiveWorkerCounter := 0

	for {
		select {
		case call := <-workerDone:
			log.Printf("blockWorkersReady - %v: received reply: %v\n", call.ServiceMethod, call)
			log.Printf("blockworkersready - %v: readyworkercounter: %v\n", call.ServiceMethod, readyWorkerCounter)

			if call.Error != nil {
				log.Printf("blockWorkersReady - %v: received error: %v\n", call.ServiceMethod, call.Error)
			} else {

				// todo check is for completion and not recovery complete
				if ssComplete, ok := call.Reply.(*ProgressSuperStep); ok && !ssComplete.IsActive {
					inactiveWorkerCounter++
					fmt.Printf("found a lazy one!!!\n")
				}

				readyWorkerCounter++
				log.Printf("blockWorkersReady - %v: %d workers ready! %d workers inactive!\n",
					call.ServiceMethod,
					readyWorkerCounter,
					inactiveWorkerCounter)
				if readyWorkerCounter == numWorkers {
					c.allWorkersReady <- superstepDone{
						allWorkersInactive: numWorkers == inactiveWorkerCounter,
						isSuccess:          true,
						ShortestPathResult: 0,
						PageRankResult:     0,
					}
					readyWorkerCounter = 0
					return
				}
			}
		}
	}
}

// TODO: test this!
func (c *Coord) UpdateCheckpoint(
	msg CheckpointMsg, reply *CheckpointMsg,
) error {
	// save the last SuperStep # checkpointed by this worker
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

func (c *Coord) Compute() (int, error) {
	// keep sending messages to workers, until everything has completed
	// need to make it concurrent; so put in separate channel

	numWorkers := len(c.queryWorkers)
	go c.blockWorkersReady(numWorkers, c.workerDoneStart)

	for {
		select {
		case notify := <-c.restartSuperStepCh:
			log.Printf("worker failed: %v\n", notify)
			c.restartCheckpoint()
			go c.blockWorkersReady(numWorkers, c.workerDoneRestart)
		case result := <-c.allWorkersReady:

			fmt.Printf("Coord: Compute: received all %d workers - compute is complete!\n", numWorkers)

			fmt.Printf("Coord-running compute with superstep: %v\n", c.superStepNumber)
			time.Sleep(3 * time.Second) // TODO: remove

			if result.allWorkersInactive {
				log.Printf("Computation is complete!")
				return -1, nil
			}

			shouldCheckPoint := c.superStepNumber%c.checkpointFrequency == 0
			// call workers query handler
			progressSuperStep := ProgressSuperStep{
				SuperStepNum: c.superStepNumber,
				IsCheckpoint: shouldCheckPoint,
			}
			fmt.Println("Coord - calling checkWorkersReady from Compute!")
			fmt.Printf("Coord: Compute: progressing super step # %d, should checkpoint %v \n",
				c.superStepNumber, shouldCheckPoint)

			c.workerDoneCompute = make(chan *rpc.Call, numWorkers)
			for _, wClient := range c.queryWorkers {
				var result ProgressSuperStep
				wClient.Go(
					"Worker.ComputeVertices", progressSuperStep, &result,
					c.workerDoneCompute,
				)
			}
			go c.blockWorkersReady(numWorkers, c.workerDoneCompute)
			c.superStepNumber += 1
		}
	}
	log.Printf("Compute: Query complete, result found\n")
	return -1, nil
}

func (c *Coord) restartCheckpoint() {
	log.Printf("Coord - restart checkpoint\n")
	checkpointNumber := c.lastCheckpointNumber
	numWorkers := len(c.queryWorkers)

	restartSuperStep := RestartSuperStep{SuperStepNumber: checkpointNumber}

	fmt.Println("Coord - calling checkWorkersReady from restartCheckpoint!")

	c.workerDoneRestart = make(chan *rpc.Call, numWorkers)
	for wId, wClient := range c.queryWorkers {
		fmt.Printf("Coord - calling RevertToLastCheckpoint on worker %v\n", wId)
		var result RestartSuperStep
		wClient.Go(
			"Worker.RevertToLastCheckpoint", restartSuperStep, &result,
			c.workerDoneRestart,
		)
	}
	c.superStepNumber = checkpointNumber + 1
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
			w.WorkerId)
		c.queryWorkers[w.WorkerId] = client
		c.workers[w.WorkerId] = client

		checkpointNumber := c.lastCheckpointNumber
		log.Printf("JoinWorker: restarting failed worker from checkpoint: %v\n", checkpointNumber)

		restartSuperStep := RestartSuperStep{SuperStepNumber: checkpointNumber}
		var result RestartSuperStep
		client.Go(
			"Worker.RevertToLastCheckpoint", restartSuperStep, &result, c.workerDoneRestart)
		log.Printf("JoinWorker: called RPC to revert to last checkpoint %v for readded worker\n", checkpointNumber)
	} else {
		c.workers[w.WorkerId] = client
		log.Printf(
			"JoinWorker: New Worker %d successfully added. %d Workers joined\n",
			w.WorkerId, len(c.workers))
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
		log.Printf("monitor: fchecker failed to connect. notifyCh nil and/or received err: %v\n", err)
	}

	log.Printf("monitor: Fcheck for Worker %d running\n", w.WorkerId)
	for {
		select {
		case notify := <-notifyCh:
			log.Printf("monitor: worker %v failed: %s\n", w.WorkerId, notify)
			c.restartSuperStepCh <- true
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
	c.restartSuperStepCh = make(chan bool, 1)
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
