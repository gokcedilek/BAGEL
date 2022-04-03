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

	workers            []uint32 // list of active worker ids
	superStepNumber    uint32
	workerToCheckpoint map[uint32]uint32
}

func NewCoord() *Coord {
	return &Coord{
		clientAPIListenAddr: "",
		workerAPIListenAddr: "",
		lostMsgsThresh:      0,
		workerToCheckpoint:  make(map[uint32]uint32),
	}
}

func (c *Coord) UpdateCheckpoint(msg CheckpointMsg, reply *CheckpointMsg) error {
	fmt.Printf("called update checkpoint with msg: %v\n", msg)
	// save the last superstep # checkpointed by this worker
	c.workerToCheckpoint[msg.WorkerId] = msg.SuperStepNumber

	// update global superstep # if needed
	allWorkersUpdated := true
	for _, v := range c.workerToCheckpoint {
		if v != msg.SuperStepNumber {
			allWorkersUpdated = false
			break
		}
	}
	fmt.Printf("coord checkpoints map: %v\n", c.workerToCheckpoint)

	if allWorkersUpdated {
		c.superStepNumber = msg.SuperStepNumber
	}

	*reply = msg
	return nil
}

func (c *Coord) JoinWorker(w WorkerNode, reply *WorkerNode) error {
	fmt.Printf("Coord: JoinWorker: Adding worker %d to chain\n", w.WorkerId)

	c.workers = append(c.workers, w.WorkerId)

	fmt.Printf("Coord: JoinWorker: Successfully added Worker %d to chain. %d Workers joined\n", w.WorkerId, len(c.workers))

	go c.monitor(w)

	// return nil for no errors
	return nil
}

func listenWorkers(workerAPIListenAddr string) {

	wlisten, err := net.Listen("tcp", workerAPIListenAddr)
	if err != nil {
		fmt.Printf("Coord: listenWorkers: Error listening: %v\n", err)
	}
	fmt.Printf("Coord: listenWorkers: listening for workers at %v\n", workerAPIListenAddr)

	for {
		conn, err := wlisten.Accept()
		if err != nil {
			fmt.Printf("Coord: listenWorkers: Error accepting worker: %v\n", err)
		}
		fmt.Printf("Coord: listenWorkers: accepted connection to worker\n")
		go rpc.ServeConn(conn) // blocks while serving connection until client hangs up
	}
}

func (c *Coord) monitor(w WorkerNode) {

	// get random port for heartbeats
	//hBeatLocalAddr, _ := net.ResolveUDPAddr("udp", strings.Split(c.WorkerAPIListenAddr, ":")[0]+":0")
	fmt.Printf("Coord: monitor: Attemping to monitor Worker %d at %v\n", w.WorkerId, w.WorkerAddr)

	epochNonce := rand.Uint64()

	notifyCh, _, err := fchecker.Start(fchecker.StartStruct{
		strings.Split(c.workerAPIListenAddr, ":")[0] + ":0",
		epochNonce,
		strings.Split(c.workerAPIListenAddr, ":")[0] + ":0",
		w.WorkerFCheckAddr,
		c.lostMsgsThresh, w.WorkerId})
	if err != nil || notifyCh == nil {
		fmt.Printf("fchecker failed to connect\n")
	}

	fmt.Printf("Coord: monitor: Fcheck for Worker %d running\n", w.WorkerId)
}

// Only returns when network or other unrecoverable errors occur
func (c *Coord) Start(clientAPIListenAddr string, workerAPIListenAddr string, lostMsgsThresh uint8, checkpointSteps uint64) error {

	c.clientAPIListenAddr = clientAPIListenAddr
	c.workerAPIListenAddr = workerAPIListenAddr
	c.lostMsgsThresh = lostMsgsThresh

	err := rpc.Register(c)
	util.CheckErr(err, fmt.Sprintf("Coord could not register RPCs"))
	fmt.Printf("Coord: Start: accepting RPCs from workers and clients\n")

	wg := sync.WaitGroup{}
	wg.Add(2)
	go listenWorkers(workerAPIListenAddr)
	// go listenClients(clientAPIListenAddr)
	wg.Wait()

	// will never return
	return nil
}
