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
	clientAPIListenAddr string
	workerAPIListenAddr string
	lostMsgsThresh      uint8
	//workers               map[uint32]string // worker id --> worker address
	workers               map[uint32]*rpc.Client
	workersMutex          sync.Mutex
	superStepNumber       uint64
	lastWorkerCheckpoints map[uint32]uint64
	workerCounter         int
	workerCounterMutex    sync.Mutex
	checkpointFrequency   int
}

func NewCoord() *Coord {
	return &Coord{
		clientAPIListenAddr:   "",
		workerAPIListenAddr:   "",
		lostMsgsThresh:        0,
		lastWorkerCheckpoints: make(map[uint32]uint64),
		workers:               make(map[uint32]*rpc.Client),
	}
}

// this is the start of the query where coord notifies workers to initialize
// state for SuperStep 0
func (c *Coord) StartQuery(q Query, reply *QueryResult) error {
	// TODO: if a query is received while another one is being processed,
	// we need to put it in a pending queue
	fmt.Printf("Coord: StartQuery: received query: %v\n", q)

	// computation
	c.workersMutex.Lock()
	workers := c.workers // workers = workers used for this query, c.workers may add new workers for next query
	c.workersMutex.Unlock()

	// call workers query handler
	startSuperStep := StartSuperStep{NumWorkers: uint8(len(workers))}
	numWorkers := len(workers)
	workerDone := make(chan *rpc.Call, len(workers))
	allWorkersReady := make(chan bool, 1)

	fmt.Printf("Coord: StartQuery: computing query %v with %d workers ready!\n", q, numWorkers)

	for _, wClient := range workers {
		var result interface{}
		wClient.Go(
			"Worker.StartQuery", startSuperStep, &result,
			workerDone,
		)
		go c.checkWorkersReady(numWorkers, workerDone, allWorkersReady)
	}

	select {
	case <-allWorkersReady:
		fmt.Printf("Coord: StartQuery: received all %d workers ready!\n", numWorkers)
	}

	// TODO: invoke another function to handle the rest of the request
	err := c.Compute()

	if err != nil {
		fmt.Println(fmt.Sprintf("Coord: Compute err: %v", err))
	}

	// TODO: for testing workers joining during query, remove
	// time.Sleep(10 * time.Second)

	reply.Query = q
	reply.Result = -1

	// return nil for no errors
	return nil
}

// check if all workers are ready to start SuperStep 0
func (c *Coord) checkWorkersReady(
	numWorkers int,
	workerDone <-chan *rpc.Call,
	allWorkersReady chan<- bool,
) {
	call := <-workerDone

	fmt.Printf("Coord: checkWorkersReady: received reply: %v\n", call.Reply)

	if call.Error != nil {
		fmt.Printf("Coord: checkWorkersReady: received error: %v\n", call.Error)
	}

	c.workerCounterMutex.Lock()
	defer c.workerCounterMutex.Unlock()

	c.workerCounter++
	if c.workerCounter == numWorkers {
		fmt.Printf("Coord: checkWorkersReady: sending all %d workers ready!\n", numWorkers)
		allWorkersReady <- true
	}
}

func (c *Coord) UpdateCheckpoint(
	msg CheckpointMsg, reply *CheckpointMsg,
) error {
	fmt.Printf("called update checkpoint with msg: %v\n", msg)
	// save the last SuperStep # checkpointed by this worker
	c.lastWorkerCheckpoints[msg.WorkerId] = msg.SuperStepNumber

	// update global SuperStep # if needed
	allWorkersUpdated := true
	for _, v := range c.lastWorkerCheckpoints {
		if v != msg.SuperStepNumber {
			allWorkersUpdated = false
			break
		}
	}
	fmt.Printf("coord checkpoints map: %v\n", c.lastWorkerCheckpoints)

	if allWorkersUpdated {
		c.superStepNumber = msg.SuperStepNumber
	}

	*reply = msg
	return nil
}

func (c *Coord) Compute() error {

	// todo

	// keep sending messages to workers, until everything has completed.. hehe
	// need to make it concurrent; so put in separate channel

	// we need to put it in a pending queue
	fmt.Printf("Coord: StartQuery: received query: %v\n", q)

	// computation
	c.workersMutex.Lock()
	workers := c.workers // workers = workers used for this query, c.workers may add new workers for next query
	c.workersMutex.Unlock()

	numWorkers := len(c.workers)

	// TODO check if all workers are finished
	for i := 0; i < 5; i++ {

		shouldCheckPoint := false

		if c.superStepNumber%uint64(c.checkpointFrequency) == 0 {
			shouldCheckPoint = true
		}

		// call workers query handler
		progressSuperStep := ProgressSuperStep{
			SuperStepNum: c.superStepNumber,
			IsCheckPoint: shouldCheckPoint,
		}

		workerDone := make(chan *rpc.Call, len(workers))
		allWorkersReady := make(chan bool, 1)

		fmt.Printf("Coord: Compute: progressing super step # %d, should checkpoint %v \n",
			c.superStepNumber, shouldCheckPoint)

		for _, wClient := range workers {
			var result interface{}
			wClient.Go(
				"Worker.ComputeVertices", progressSuperStep, &result,
				workerDone,
			)
			go c.checkWorkersReady(numWorkers, workerDone, allWorkersReady)
		}

		select {
		case <-allWorkersReady:
			fmt.Printf("Coord: Compute: received all %d workers compute complete!\n", numWorkers)
		}

	}
	// todo find out if all workers are finished

	return nil // todo
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

	c.workersMutex.Lock()
	c.workers[w.WorkerId] = client
	c.workersMutex.Unlock()

	fmt.Printf(
		"Coord: JoinWorker: Successfully added Worker %d. %d Workers joined\n",
		w.WorkerId, len(c.workers),
	)

	go c.monitor(w)

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

	fmt.Printf("Coord: monitor: Fcheck for Worker %d running\n", w.WorkerId)
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
