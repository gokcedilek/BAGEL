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
	db, err := sql.Open(
		"sqlite3", fmt.Sprintf("checkpoints%v.db", w.config.WorkerId),
	)
	if err != nil {
		log.Printf("getConnection database error: %v\n", err)
		return nil, err
	}
	return db, nil
}

func (w *Worker) initializeCheckpoints() error {
	db, err := w.getConnection()

	if err != nil {
		log.Printf("initializeCheckpoints: connection error: %v\n", err)
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
		log.Printf("initializeCheckpoints: Failed execute command: %v\n", err)
		return err
	}

	// reset checkpoints
	if _, err := db.Exec("delete from checkpoints"); err != nil {
		log.Printf("initializeCheckpoints: Failed execute command: %v\n", err)
		return err
	}

	return nil
}

func (w *Worker) checkpoint(superStepNum uint64) Checkpoint {
	checkPointState := make(map[uint64]VertexCheckpoint)

	for k, v := range w.Vertices {
		checkPointState[k] = VertexCheckpoint(*v)
	}

	return Checkpoint{
		SuperStepNumber:    superStepNum,
		CheckpointState:    checkPointState,
		NextSuperStepState: w.NextSuperStep,
	}
}

func (w *Worker) storeCheckpoint(checkpoint Checkpoint) (Checkpoint, error) {
	db, err := w.getConnection()
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()

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
		log.Printf("storeCheckpoint: error inserting into db: %v\n", err)
	}

	// notify coord about the latest checkpoint saved
	coordClient, err := util.DialRPC(w.config.CoordAddr)
	util.CheckErr(
		err,
		"worker %v could not dial coord addr %v\n", w.config.WorkerAddr,
		w.config.CoordAddr,
	)

	checkpointMsg := CheckpointMsg{
		SuperStepNumber: checkpoint.SuperStepNumber,
		WorkerId:        w.config.WorkerId,
	}

	var reply CheckpointMsg
	log.Printf(
		"storeCheckpoints: calling coord with checkpointMsg: %v\n",
		checkpointMsg,
	)
	err = coordClient.Call("Coord.UpdateCheckpoint", checkpointMsg, &reply)
	util.CheckErr(
		err,
		"storeCheckpoints: worker %v could not call UpdateCheckpoint",
		w.config.WorkerAddr,
	)

	return checkpoint, nil
}

func (w *Worker) retrieveCheckpoint(superStepNumber uint64) (
	Checkpoint, error,
) {
	db, err := w.getConnection()
	if err != nil {
		os.Exit(1)
	}
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
