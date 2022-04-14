package bagel

import (
	"fmt"
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
	workers               map[uint32]*rpc.Client // worker id --> worker connection
	queryWorkers          map[uint32]*rpc.Client // workers in use for current query - will be updated at start of query
	workersMutex          sync.Mutex
	lastCheckpointNumber  uint64
	lastWorkerCheckpoints map[uint32]uint64
	workerCounter         int
	workerCounterMutex    sync.Mutex
	checkpointFrequency   int
	superStepNumber       uint64
	workerDone            chan *rpc.Call
	allWorkersReady       chan bool
	//notifyCh              <-chan fchecker.FailureDetected
	restartSuperStepCh chan bool
}

func NewCoord() *Coord {
	return &Coord{
		clientAPIListenAddr:   "",
		workerAPIListenAddr:   "",
		lostMsgsThresh:        0,
		lastWorkerCheckpoints: make(map[uint32]uint64),
		workers:               make(map[uint32]*rpc.Client),
		checkpointFrequency:   1,
	}
}

// this is the start of the query where coord notifies workers to initialize
// state for SuperStep 0
func (c *Coord) StartQuery(q Query, reply *QueryResult) error {
	fmt.Printf("Coord: StartQuery: received query: %v\n", q)

	if len(c.workers) == 0 {
		fmt.Printf("Coord: StartQuery: received query, no workers available, will block until workers join: %v\n", q)
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
	startSuperStep := StartSuperStep{NumWorkers: uint8(len(c.queryWorkers))}
	numWorkers := len(c.queryWorkers)
	c.workerDone = make(chan *rpc.Call, numWorkers)
	c.allWorkersReady = make(chan bool, 1)

	fmt.Printf("Coord: StartQuery: computing query %v with %d workers ready!\n", q, numWorkers)

	go c.checkWorkersReady(numWorkers)
	for _, wClient := range c.queryWorkers {
		var result interface{}
		wClient.Go(
			"Worker.StartQuery", startSuperStep, &result,
			c.workerDone,
		)
	}

	select {
	case <-c.allWorkersReady:
		fmt.Printf("Coord: StartQuery: received all %d workers ready!\n", numWorkers)
	}

	// TODO: invoke another function to handle the rest of the request
	result, err := c.Compute()
	if err != nil {
		fmt.Println(fmt.Sprintf("Coord: Compute err: %v", err))
	}

	reply.Query = q
	reply.Result = result

	//c.queryWorkers = nil

	// return nil for no errors
	return nil
}

// check if all workers are notified by coord
func (c *Coord) checkWorkersReady(
	numWorkers int) {

	for {
		select {
		case call := <-c.workerDone:
			fmt.Printf("Coord: checkWorkersReady: received reply: %v\n", call.Reply)

			if call.Error != nil {
				fmt.Printf("Coord: checkWorkersReady: received error: %v\n", call.Error)
			} else {
				c.workerCounterMutex.Lock()
				c.workerCounter++
				c.workerCounterMutex.Unlock()
				fmt.Printf("Coord: checkWorkersReady: %d workers ready!\n", c.workerCounter)
			}

			if c.workerCounter == numWorkers {
				fmt.Printf("Coord: checkWorkersReady: sending all %d workers ready!\n", numWorkers)
				c.allWorkersReady <- true
				c.workerCounterMutex.Lock()
				c.workerCounter = 0
				c.workerCounterMutex.Unlock()
				return
			}
		}
	}
}

// TODO: test this!
func (c *Coord) UpdateCheckpoint(
	msg CheckpointMsg, reply *CheckpointMsg,
) error {
	//fmt.Printf("called update checkpoint with msg: %v\n", msg)
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
	//fmt.Printf("coord checkpoints map: %v\n", c.lastWorkerCheckpoints)

	if allWorkersUpdated {
		c.lastCheckpointNumber = msg.SuperStepNumber
		//fmt.Printf("coord all workers updated: %v\n", c.lastCheckpointNumber)
	}

	*reply = msg
	return nil
}

func (c *Coord) Compute() (int, error) {
	// keep sending messages to workers, until everything has completed
	// need to make it concurrent; so put in separate channel

	fmt.Printf("Coord: in compute\n")
	//numWorkers := len(c.queryWorkers)

	// TODO check if all workers are finished, currently returns placeholder result after 5 supersteps
	for {
		select {
		case notify := <-c.restartSuperStepCh:
			fmt.Printf("worker failed: %s\n", notify)
			c.restartCheckpoint()
		default:
			fmt.Printf("Coord-running compute\n")
			time.Sleep(3 * time.Second)
		}
	}
	//for i := 0; i < 5; i++ {
	//	// for {
	//
	//	fmt.Printf("Coord - compute running i=%v\n", i)
	//	//shouldCheckPoint := c.superStepNumber%uint64(c.checkpointFrequency) == 0
	//
	//	// call workers query handler
	//	//progressSuperStep := ProgressSuperStep{
	//	//	SuperStepNum: c.superStepNumber,
	//	//	IsCheckpoint: shouldCheckPoint,
	//	//}
	//	//
	//	////c.workerDone = make(chan *rpc.Call, numWorkers)
	//	////c.allWorkersReady = make(chan bool, 1)
	//	//go c.checkWorkersReady(numWorkers)
	//	//
	//	//fmt.Printf("Coord: Compute: progressing super step # %d, should checkpoint %v \n",
	//	//	c.superStepNumber, shouldCheckPoint)
	//	//
	//	//for _, wClient := range c.queryWorkers {
	//	//	var result ProgressSuperStep
	//	//	wClient.Go(
	//	//		"Worker.ComputeVertices", progressSuperStep, &result,
	//	//		c.workerDone,
	//	//	)
	//	//}
	//
	//	// TODO: for testing workers joining during query, remove
	//	time.Sleep(3 * time.Second)
	//
	//	//select {
	//	//case <-c.allWorkersReady:
	//	//	fmt.Printf("Coord: Compute: received all %d workers - compute is complete!\n", numWorkers)
	//	//}
	//
	//	c.superStepNumber += 1
	//
	//}
	fmt.Printf("Coord: Compute: compute query returning result\n")
	return -1, nil
}

func (c *Coord) restartCheckpoint() {
	fmt.Printf("Coord - restart checkpoint\n")
	checkpointNumber := c.lastCheckpointNumber

	restartSuperStep := RestartSuperStep{SuperStepNumber: checkpointNumber}

	numWorkers := len(c.queryWorkers)

	go c.checkWorkersReady(numWorkers)
	for wId, wClient := range c.queryWorkers {
		fmt.Printf("Coord - calling RevertToLastCheckpoint on worker %v\n", wId)
		var result RestartSuperStep
		wClient.Go(
			"Worker.RevertToLastCheckpoint", restartSuperStep, &result,
			c.workerDone,
		)
	}

	select {
	case <-c.allWorkersReady:
		fmt.Printf("Coord: restart checkpoint: received all %d workers!\n", numWorkers)
		// continue query from the last checkpoint
		c.superStepNumber = c.lastCheckpointNumber + 1 // reset superstep number
		//result, err := c.Compute()
		//if err != nil {
		//	fmt.Println(fmt.Sprintf("Coord: Compute err: %v", err))
		//}
		// TODO: ask about this part: after setting c.superstepNumber, call compute?
	}

}

func (c *Coord) JoinWorker(w WorkerNode, reply *WorkerNode) error {
	fmt.Printf("Coord: JoinWorker: Adding worker %d\n", w.WorkerId)

	// TODO: needs to block while there is an ongoing query

	client, err := util.DialRPC(w.WorkerListenAddr)
	if err != nil {
		fmt.Printf(
			"coord could not dial worker addr %v, err: %v\n",
			w.WorkerListenAddr, err,
		)
		return err
	}

	go c.monitor(w)

	if _, ok := c.queryWorkers[w.WorkerId]; ok {
		// joining worker is restarted process of failed worker used in current query
		fmt.Printf(
			"Coord: JoinWorker: Successfully re-added Worker %d after failure\n",
			w.WorkerId)
		c.queryWorkers[w.WorkerId] = client // TODO: do we also need to add to c.workers?
		c.workers[w.WorkerId] = client

		checkpointNumber := c.lastCheckpointNumber
		fmt.Printf("Coord: JoinWorker: restarting re-added worker from checkpoint: %v\n", checkpointNumber)

		restartSuperStep := RestartSuperStep{SuperStepNumber: checkpointNumber}
		var result RestartSuperStep
		client.Go(
			"Worker.RevertToLastCheckpoint", restartSuperStep, &result, c.workerDone)
		fmt.Printf("Coord: JoinWorker: called RPC to revert to last checkpoint %v for readded worker\n", checkpointNumber)
	} else {
		c.workers[w.WorkerId] = client
		fmt.Printf(
			"Coord: JoinWorker: Successfully added Worker %d. %d Workers joined\n",
			w.WorkerId, len(c.workers))
	}

	// return nil for no errors
	return nil
}

func listenWorkers(workerAPIListenAddr string) {

	wlisten, err := net.Listen("tcp", workerAPIListenAddr)
	if err != nil {
		fmt.Printf("Coord: listenWorkers: Error listening: %v\n", err)
	}
	fmt.Printf(
		"Coord: listenWorkers: listening for workers at %v\n",
		workerAPIListenAddr,
	)

	for {
		conn, err := wlisten.Accept()
		if err != nil {
			fmt.Printf(
				"Coord: listenWorkers: Error accepting worker: %v\n", err,
			)
		}
		fmt.Printf("Coord: listenWorkers: accepted connection to worker\n")
		go rpc.ServeConn(conn) // blocks while serving connection until client hangs up
	}
}

func (c *Coord) monitor(w WorkerNode) {

	// get random port for heartbeats
	//hBeatLocalAddr, _ := net.ResolveUDPAddr("udp", strings.Split(c.WorkerAPIListenAddr, ":")[0]+":0")
	fmt.Printf(
		"Coord: monitor: Attemping to monitor Worker %d at %v\n", w.WorkerId,
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
		fmt.Printf("fchecker failed to connect\n")
	}
	//c.notifyCh = notifyCh

	fmt.Printf("Coord: monitor: Fcheck for Worker %d running\n", w.WorkerId)
	for {
		select {
		case notify := <-notifyCh: // make notifyCh part of coord state, add this for loop to the beg of compute()
			fmt.Printf("worker %v failed: %s\n", w.WorkerId, notify)
			//c.restartCheckpoint()
			c.restartSuperStepCh <- true
		}
	}
}

func listenClients(clientAPIListenAddr string) {

	wlisten, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		fmt.Printf("Coord: listenClients: Error listening: %v\n", err)
	}
	fmt.Printf(
		"Coord: listenClients: listening for clients at %v\n",
		clientAPIListenAddr,
	)

	for {
		conn, err := wlisten.Accept()
		if err != nil {
			fmt.Printf(
				"Coord: listenClients: Error accepting client: %v\n", err,
			)
		}
		fmt.Printf("Coord: listenClients: accepted connection to client\n")
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

	err := rpc.Register(c)
	util.CheckErr(err, fmt.Sprintf("Coord could not register RPCs"))
	fmt.Printf("Coord: Start: accepting RPCs from workers and clients\n")

	wg := sync.WaitGroup{}
	wg.Add(2)
	go listenWorkers(workerAPIListenAddr)
	go listenClients(clientAPIListenAddr)
	wg.Wait()

	// will never return
	return nil
}
