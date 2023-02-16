package bagel

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"project/util"
)

type Checkpoint struct {
	SuperStepNumber    uint64
	CheckpointState    map[uint64]VertexCheckpoint
	NextSuperStepState *SuperStep
}

func (w *Worker) getConnection() (*sql.DB, error) {
	// config's workerID to avoid conflict w/ replica
	db, err := sql.Open(
		"sqlite3", fmt.Sprintf("checkpoints%v.db", w.config.WorkerId),
	)
	if err != nil {
		log.Printf("getConnection: database error: %v\n", err)
		return nil, err
	}
	return db, nil
}

func (w *Worker) resetCheckpoints() error {
	// reset checkpoints
	db, err := w.getConnection()

	if err != nil {
		log.Printf(
			"initializeCheckpoints: connection error: %v"+
				"\n", err,
		)
		return err
	}

	if _, err := db.Exec("delete from checkpoints"); err != nil {
		log.Printf(
			"initializeCheckpoints: Failed execute"+
				" command: %v\n", err,
		)
		return err
	}
	return nil
}

func (w *Worker) initializeCheckpoints() error {
	db, err := w.getConnection()

	if err != nil {
		log.Printf(
			"initializeCheckpoints: connection error: %v"+
				"\n", err,
		)
		return err
	}

	//goland:noinspection SqlDialectInspection
	const createCheckpoints string = `
	  CREATE TABLE IF NOT EXISTS checkpoints (
	  lastCheckpointNumber INTEGER NOT NULL PRIMARY KEY, 
	  checkpointState BLOB NOT NULL,
	  nextSuperStepState BLOB NOT NULL
	  );`

	if _, err := db.Exec(createCheckpoints); err != nil {
		log.Printf(
			"initializeCheckpoints: Failed execute"+
				" command: %v\n", err,
		)
		return err
	}

	return nil
}

func (w *Worker) checkpoint(superStepNum uint64) Checkpoint {
	checkPointState := make(map[uint64]VertexCheckpoint)

	for k, v := range w.Vertices {
		checkPointState[k] = VertexCheckpoint{
			Id:             v.Id,
			Neighbors:      v.Neighbors,
			PreviousValues: v.PreviousValues,
			CurrentValue:   v.CurrentValue,
		}
	}

	return Checkpoint{
		SuperStepNumber:    superStepNum,
		CheckpointState:    checkPointState,
		NextSuperStepState: w.NextSuperStep,
	}
}

/*
	Main Worker's RPC function.
*/
func (w *Worker) TransferCheckpointToReplica(superstep uint64, response *uint64) error {
	checkpoint, err := w.retrieveCheckpoint(superstep)
	log.Printf("Worker %v transfering checkpoint. Checkpoint (SS#: %v):\n\t%v", superstep, w.LogicalId, checkpoint)
	if err != nil {
		return err
	}
	err = w.storeCheckpointReplica(checkpoint)
	return err
}

/*
	TODO: rename this function
	Replica's RPC function. Invoked by Main Worker
*/
func (w *Worker) ReceiveCheckpointOnReplica(checkpoint Checkpoint, res *Checkpoint) error {
	log.Printf("Worker %v is replica: %v received RPC call to checkpoint. %v", w.LogicalId, w.Replica, checkpoint)
	err := w.initializeCheckpoints()
	util.CheckErr(err, "Failed to initialize checkpoint for replica")
	_, err = w.storeCheckpoint(checkpoint, true)
	util.CheckErr(err, "Failed to store checkpoint for replica")
	return nil
}

/*
	Invoked by main worker
*/
func (w *Worker) storeCheckpointReplica(checkpoint Checkpoint) error {
	if w.ReplicaClient == nil {
		client, err := util.DialRPC(w.Replica.WorkerListenAddr)
		util.CheckErr(err, "Store CP on Replica - could not connect to replica.\n\tError: %v", err)
		w.ReplicaClient = client
	}

	var response Checkpoint
	err := w.ReplicaClient.Call("Worker.ReceiveCheckpointOnReplica", checkpoint, &response)
	util.CheckErr(
		err, "Store Checkpoint On Replica: Worker %v could not sync with replica: %v\n",
		w.LogicalId, err,
	)
	return nil
}

func (w *Worker) storeCheckpoint(
	checkpoint Checkpoint,
	isInvokedByReplica bool,
) (Checkpoint, error) {
	db, err := w.getConnection()
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()
	// clear larger checkpoints that were saved
	if _, err := db.Exec(
		"delete from checkpoints where lastCheckpointNumber"+
			">=?", checkpoint.SuperStepNumber,
	); err != nil {
		log.Fatalf(
			"storeCheckpoint: Failed execute"+
				" command: %v\n", err,
		)
	}

	var buf bytes.Buffer
	if err = gob.NewEncoder(&buf).Encode(checkpoint.CheckpointState); err != nil {
		log.Printf("storeCheckpoint: encode error: %v\n", err)
	}

	var buf2 bytes.Buffer
	if err = gob.NewEncoder(&buf2).Encode(checkpoint.NextSuperStepState); err != nil {
		log.Printf("storeCheckpoint: encode error: %v\n", err)
	}

	_, err = db.Exec(
		"INSERT INTO checkpoints VALUES(?,?,?)",
		checkpoint.SuperStepNumber,
		buf.Bytes(),
		buf2.Bytes(),
	)
	if err != nil {
		log.Fatalf(
			"storeCheckpoint: error inserting into db: %v"+
				"\n", err,
		)
	}

	// If 'this' == replica worker, want to store checkpoint
	// 	but do NOT want to
	//		- notify coord about status
	//		- further call replica store checkpoint.
	if isInvokedByReplica {
		return checkpoint, nil
	}

	// If 'this' == main worker, want to instruct replica worker
	//	to store checkpoint
	// **NOTE** main worker may NOT have replica
	if w.Replica != (WorkerNode{}) {
		err = w.storeCheckpointReplica(checkpoint)
		util.CheckErr(
			err,
			"storeCheckpoint: worker %v could not sync checkpoint with"+
				" replica\n", w.config.WorkerAddr,
		)
	}

	// notify coord about the latest checkpoint saved
	coordClient, err := util.DialRPC(w.config.CoordAddr)
	util.CheckErr(
		err,
		"storeCheckpoint: worker %v could not dial coord addr %v"+
			"\n", w.config.WorkerAddr,
		w.config.CoordAddr,
	)

	checkpointMsg := CheckpointMsg{
		SuperStepNumber: checkpoint.SuperStepNumber,
		WorkerId:        w.LogicalId,
	}

	var reply CheckpointMsg
	err = coordClient.Call("Coord.UpdateCheckpoint", checkpointMsg, &reply)
	util.CheckErr(
		err,
		"storeCheckpoint: worker %v could not call"+
			" UpdateCheckpoint",
		w.config.WorkerAddr,
	)
	log.Printf("!!!!!!! worker %v called update cp with superstep %v\n", w.LogicalId, checkpoint.SuperStepNumber)

	return checkpoint, nil
}

func (w *Worker) retrieveCheckpoint(superStepNumber uint64) (
	Checkpoint, error,
) {
	db, err := w.getConnection()
	util.CheckErr(err, "Retrieve Checkpoint - failed to establish connection with db")
	defer db.Close()

	res := db.QueryRow(
		"SELECT * FROM checkpoints WHERE lastCheckpointNumber=?",
		superStepNumber,
	)
	checkpoint := Checkpoint{}
	var buf []byte
	var buf2 []byte
	if err := res.Scan(
		&checkpoint.SuperStepNumber, &buf, &buf2,
	); err == sql.ErrNoRows {
		log.Printf("retrieveCheckpoint: scan error: %v\n", err)
		return Checkpoint{}, err
	}

	var checkpointState map[uint64]VertexCheckpoint
	err = gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&checkpointState)
	if err != nil {
		log.Printf(
			"retrieveCheckpoint: decode error: %v, tmp: %v\n", err,
			checkpointState,
		)
		return Checkpoint{}, err
	}
	checkpoint.CheckpointState = checkpointState

	var nextSuperStepState *SuperStep
	err = gob.NewDecoder(bytes.NewBuffer(buf2)).Decode(&nextSuperStepState)
	if err != nil {
		log.Printf(
			"retrieveCheckpoint: decode error: %v, tmp: %v\n", err,
			checkpointState,
		)
		return Checkpoint{}, err
	}
	checkpoint.NextSuperStepState = nextSuperStepState

	return checkpoint, nil
}
