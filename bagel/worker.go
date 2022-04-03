package bagel

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"net"
	"net/rpc"
	"os"
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

const SUPERSTEPNUMBER = 0

type NeighbourVertex struct {
	vertexId uint64
}

// Vertex stores intermediate calculation data about the vertex
type Vertex struct {
	neighbors    []NeighbourVertex
	currentValue float64
	messages     []Message
	isActive     bool
	workerAddr   string
	vertexId     uint64
}

// Message represents an arbitrary message sent during calculation
// msgType is used to distinguish between which type of Message was sent
//type Message struct {
//	MsgType        int
//	SourceVertexId int64
//}
//
//// ShortestPathsMessage represents a message for the Shortest Paths computation
//type ShortestPathsMessage struct {
//	Message
//	pathLength int
//}
//
//// PageRankMessage represents a message for the Page Rank computation
//type PageRankMessage struct {
//	Message
//	flowValue float64
//}
type Message struct {
	Value          uint64
	SourceVertexId uint64
}

type ShortestPathsMessage Message
type PageRankMessage Message

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
	partition        []Vertex
	SuperStepNumber  uint32
}

type VertexCheckpoint struct {
	//VertexId     uint64
	CurrentValue float64
	Messages     []Message
	IsActive     bool
}

type Checkpoint struct {
	SuperStepNumber uint32
	//CheckpointState []VertexCheckpoint
	CheckpointState map[uint64]VertexCheckpoint
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

func dbSetup() (*sql.DB, error) {
	const createCheckpoints string = `
	  CREATE TABLE IF NOT EXISTS checkpoints (
	  superStepNumber INTEGER NOT NULL PRIMARY KEY,
	  checkpointState BLOB NOT NULL
	  );`
	db, err := sql.Open("sqlite3", "checkpoints.db")
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		return nil, err
	}

	if _, err := db.Exec(createCheckpoints); err != nil {
		fmt.Printf("Failed execute command: %v\n", err)
		return nil, err
	}

	return db, nil
}

func (w *Worker) storeCheckpoint(checkpoint Checkpoint) (Checkpoint, error) {
	db, err := dbSetup()
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()

	var buf bytes.Buffer
	if err = gob.NewEncoder(&buf).Encode(checkpoint.CheckpointState); err != nil {
		fmt.Printf("encode error: %v\n", err)
	}

	_, err = db.Exec("INSERT INTO checkpoints VALUES(?,?)", checkpoint.SuperStepNumber, buf.Bytes())
	if err != nil {
		fmt.Printf("error inserting into db: %v\n", err)
	}
	fmt.Printf("inserted ssn: %v, buf: %v\n", checkpoint.SuperStepNumber, buf.Bytes())

	// notify coord about the latest checkpoint saved
	coordClient, err := util.DialRPC(w.coordAddr)
	util.CheckErr(err, fmt.Sprintf("worker %v could not dial coord addr %v\n", w.WorkerAddr, w.coordAddr))

	checkpointMsg := CheckpointMsg{
		SuperStepNumber: checkpoint.SuperStepNumber,
		WorkerId:        w.WorkerId,
	}

	var reply CheckpointMsg
	fmt.Printf("calling coord update cp: %v\n", checkpointMsg)
	err = coordClient.Call("Coord.UpdateCheckpoint", checkpointMsg, &reply)
	util.CheckErr(err, fmt.Sprintf("worker %v could not call UpdateCheckpoint", w.WorkerAddr))

	fmt.Printf("called coord update cp: %v\n", reply)

	return checkpoint, nil
}

func (w *Worker) retrieveCheckpoint(superStepNumber uint32) (Checkpoint, error) {
	db, err := dbSetup()
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()

	res := db.QueryRow("SELECT * FROM checkpoints WHERE superStepNumber=?", superStepNumber)
	checkpoint := Checkpoint{}
	var buf []byte
	if err := res.Scan(&checkpoint.SuperStepNumber, &buf); err == sql.ErrNoRows {
		fmt.Printf("scan error: %v\n", err)
		return Checkpoint{}, err
	}
	fmt.Printf("buf: %v\n", buf)
	var checkpointState map[uint64]VertexCheckpoint
	err = gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&checkpointState)
	if err != nil {
		fmt.Printf("decode error: %v, tmp: %v\n", err, checkpointState)
		return Checkpoint{}, err
	}
	checkpoint.CheckpointState = checkpointState

	fmt.Printf("read ssn: %v, state: %v\n", checkpoint.SuperStepNumber, checkpoint.CheckpointState)

	return checkpoint, nil
}

func (w *Worker) RevertToLastCheckpoint(req CheckpointMsg, reply *Checkpoint) error {
	checkpoint, err := w.retrieveCheckpoint(req.SuperStepNumber)
	if err != nil {
		fmt.Printf("error retrieving checkpoint: %v\n", err)
		return err
	}
	fmt.Printf("retrieved checkpoint: %v\n", checkpoint)

	w.SuperStepNumber = checkpoint.SuperStepNumber
	for _, v := range w.partition {
		if state, found := checkpoint.CheckpointState[v.vertexId]; found {
			fmt.Printf("found state: %v\n", state)
			v.currentValue = state.CurrentValue
			v.isActive = state.IsActive
			v.messages = state.Messages
		}
	}
	// TODO: call compute wrapper with new superstep #
	*reply = checkpoint
	return nil
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
	w.coordAddr = coordAddr

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

	// test sqlite
	w.partition = []Vertex{
		{
			neighbors:    nil,
			currentValue: 5,
			messages: []Message{{
				Value: 10, SourceVertexId: 2}, {
				Value: 10, SourceVertexId: 2}},
			isActive:   true,
			workerAddr: "test addr",
			vertexId:   1,
		}, {
			neighbors:    nil,
			currentValue: 10,
			messages: []Message{{
				Value: 15, SourceVertexId: 1}, {
				Value: 15, SourceVertexId: 1}},
			isActive:   true,
			workerAddr: "test addr 2",
			vertexId:   2,
		},
	}
	checkpoint := Checkpoint{SuperStepNumber: 0, CheckpointState: make(map[uint64]VertexCheckpoint)}
	for _, v := range w.partition {
		vertexCheckpoint := VertexCheckpoint{
			//VertexId:     v.vertexId,
			CurrentValue: v.currentValue,
			Messages:     v.messages,
			IsActive:     v.isActive,
		}
		//checkpoint.CheckpointState = append(checkpoint.CheckpointState, vertexCheckpoint)
		checkpoint.CheckpointState[v.vertexId] = vertexCheckpoint
	}

	checkpoint, err = w.storeCheckpoint(checkpoint)
	fmt.Printf("stored checkpoint: %v\n", checkpoint)

	checkpoint, err = w.retrieveCheckpoint(0)
	fmt.Printf("retrieved checkpoint: %v\n", checkpoint)

	wg.Wait()

	return nil
}
