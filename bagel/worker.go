package bagel

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	fchecker "project/fcheck"
	"project/util"
	"sync"
)

// Message represents an arbitrary message sent during calculation
// value has a dynamic type based on the messageType
type Message struct {
	messageType    string
	superStepNum   uint64
	sourceVertexId uint64
	destVertexId   uint64
	value          interface{}
}

type WorkerNode struct {
	WorkerId         uint32
	WorkerAddr       string
	WorkerFCheckAddr string
	// TODO: add more fields as needed
}

type WorkerConfig struct {
	WorkerId              uint32
	CoordAddr             string
	WorkerAddr            string
	WorkerListenAddr      string
	FCheckAckLocalAddress string
}

type Worker struct {
	// Worker state may go here
	WorkerId              uint32
	WorkerAddr            string
	WorkerListenAddr      string
	CoordAddr             string
	FCheckAckLocalAddress string
	SuperStep             SuperStep
	Vertices              []Vertex
}

type SuperStep struct {
	Id           uint64
	QueryType    string
	Messages     []Message
	Outgoing     map[uint32]uint64
	IsCheckpoint bool
}

func NewWorker(config WorkerConfig) *Worker {
	return &Worker{
		WorkerId:              config.WorkerId,
		WorkerAddr:            config.WorkerAddr,
		WorkerListenAddr:      config.WorkerListenAddr,
		CoordAddr:             config.CoordAddr,
		FCheckAckLocalAddress: config.FCheckAckLocalAddress,
		SuperStep: SuperStep{
			Id:           0,
			Messages:     nil,
			Outgoing:     nil,
			IsCheckpoint: false,
		},
	}
}

func (w *Worker) startFCheckHBeat(workerId uint32, ackAddress string) string {
	fmt.Printf("Starting fcheck for worker %d\n", workerId)

	fcheckConfig := fchecker.StartStruct{
		AckLocalIPAckLocalPort: ackAddress,
	}

	_, addr, err := fchecker.Start(fcheckConfig)

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
/*
func (w *Worker) register() {
	handler := rpc.NewServer()
	err := handler.Register(w)
	fmt.Printf("register: Worker %v - register error: %v\n", w.WorkerId, err)

	go w.listenCoord(handler)
}
*/

func (w *Worker) Start() error {
	// set Worker state
	if w.WorkerAddr == "" {
		return errors.New("Failed to start worker. Please initialize worker before calling Start")
	}

	// connect to the coord node
	conn, err := util.DialTCPCustom(w.WorkerListenAddr, w.CoordAddr)
	util.CheckErr(err, fmt.Sprintf("Worker %d failed to Dial Coordinator - %s\n", w.WorkerId, w.CoordAddr))

	defer conn.Close()
	coordClient := rpc.NewClient(conn)

	hBeatAddr := w.startFCheckHBeat(w.WorkerId, w.FCheckAckLocalAddress)
	fmt.Printf("hBeatAddr for Worker %d is %v\n", w.WorkerId, hBeatAddr)

	workerNode := WorkerNode{w.WorkerId, w.WorkerAddr, hBeatAddr}

	var response WorkerNode
	err = coordClient.Call("Coord.JoinWorker", workerNode, &response)
	util.CheckErr(err, fmt.Sprintf("Worker %v could not join\n", w.WorkerId))

	// register Worker for RPC
	// w.register()

	fmt.Printf("Worker: Start: worker %v joined to coord successfully\n", w.WorkerId)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// go wait for work to do

	wg.Wait()

	return nil
}

func (w *Worker) ComputeVertices(args SuperStep, resp *SuperStep) error {
	// todo flag to indicate SS or use Superstep # to recover?

	if args.Id != w.SuperStep.Id+1 {
		// todo this is recovery
	}
	// resume after recovery

	for _, vertex := range w.Vertices {
		messageMap := vertex.Compute()
		w.updateMessageMap(messageMap)
	}

	if args.IsCheckpoint {
		// todo checkpoint
	}

	resp = &w.SuperStep
	return nil
}

func (w *Worker) SampleRPC(args Message, resp *Message) error {
	return nil
}

// todo should this be RPC?
func (w *Worker) ReceiveWorkerMessages(args Message, resp *Message) error {
	if w.SuperStep.Id+1 != args.superStepNum {
		return nil // ignore msgs not for next superstep
	}

	return nil
}

func (w *Worker) updateMessageMap(msgMap map[uint32]uint64) {
	for worker, numMessages := range msgMap {
		w.SuperStep.Outgoing[worker] += numMessages
	}
}
