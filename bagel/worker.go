package bagel

import (
	"fmt"
	"net"
	"net/rpc"
	fchecker "project/fcheck"
	"project/util"
	"strings"
	"sync"
)

// constants are used as msgType for the messages
const (
	SHORTEST_PATHS = iota + 1
	PAGE_RANK
)

// Vertex stores intermediate calculation data about the vertex
type Vertex struct {
	neighbors    []Vertex
	currentValue float64
	messages     []Message
	isActive     bool
	workerAddr   string
}

// Message represents an arbitrary message sent during calculation
// msgType is used to distinguish between which type of Message was sent
type Message struct {
	msgType        int
	sourceVertexId int64
}

// ShortestPathsMessage represents a message for the Shortest Paths computation
type ShortestPathsMessage struct {
	Message
	pathLength int
}

// PageRankMessage represents a message for the Page Rank computation
type PageRankMessage struct {
	Message
	flowValue float64
}

type WorkerConfig struct {
	WorkerId         uint32
	CoordAddr        string
	WorkerAddr       string
	WorkerListenAddr string
}

type Worker struct {
	// Worker state may go here
	WorkerId         uint32
	WorkerAddr       string
	WorkerListenAddr string
	coordAddr        string
}

func NewWorker() *Worker {
	return &Worker{}
}

func (w *Worker) startFCheckHBeat(workerId uint32) string {
	fmt.Printf("Starting fcheck for %d\n", workerId)

	//hBeatLocalAddr, err := net.ResolveUDPAddr("udp", strings.Split(s.WorkerListenAddr, ":")[0]+":0")
	//util.CheckErr(err, fmt.Sprintf("Worker %d heartbeat on %v", WorkerId, hBeatLocalAddr))

	_, addr, err := fchecker.Start(fchecker.StartStruct{strings.Split(w.WorkerListenAddr, ":")[0] + ":0", 0,
		"", "", 0, workerId})
	if err != nil {
		fchecker.Stop()
		util.CheckErr(err, fmt.Sprintf("fchecker for Worker %d failed", workerId))
	}

	return addr
}

func (w *Worker) listenCoord(handler *rpc.Server) {
	listenAddr, err := net.ResolveTCPAddr("tcp", w.WorkerListenAddr)
	util.CheckErr(err, fmt.Sprintf("Worker %v could not resolve WorkerListenAddr: %v", w.WorkerId, w.WorkerListenAddr))
	listen, err := net.ListenTCP("tcp", listenAddr)
	util.CheckErr(err, fmt.Sprintf("Worker %v could not listen on listenAddr: %v", w.WorkerId, listenAddr))

	for {
		conn, err := listen.Accept()
		util.CheckErr(err, fmt.Sprintf("Worker %v could not accept connections\n", w.WorkerId))
		go handler.ServeConn(conn)
	}
}

// create a new RPC Worker instance for the current Worker
func (w *Worker) register() {
	handler := rpc.NewServer()
	err := handler.Register(w)
	fmt.Printf("register: Worker %v - register error: %v\n", w.WorkerId, err)

	go w.listenCoord(handler)
}

func (w *Worker) Start(workerId uint32, coordAddr string, workerAddr string, workerListenAddr string) error {
	// set Worker state
	w.WorkerId = workerId
	w.WorkerListenAddr = workerListenAddr

	// connect to the coord node
	conn, err := util.DialTCPCustom(workerAddr, coordAddr)

	defer conn.Close()
	coordClient := rpc.NewClient(conn)

	hBeatAddr := w.startFCheckHBeat(workerId)
	fmt.Printf("hBeatAddr for Worker %d is %v\n", workerId, hBeatAddr)

	workerNode := WorkerNode{w.WorkerId, w.WorkerAddr, hBeatAddr}

	var response WorkerNode
	err = coordClient.Call("Coord.JoinWorker", workerNode, &response)
	util.CheckErr(err, fmt.Sprintf("Worker %v could not join Worker\n", workerId))

	// register Worker for RPC
	w.register()

	fmt.Printf("Worker: Start: worker %v joined to coord successfully\n", workerId)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// go wait for work to do

	wg.Wait()

	return nil
}
