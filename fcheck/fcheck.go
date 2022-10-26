/*

This package specifies the API to the failure checking library to be
used in assignment 2 of UBC CS 416 2021W2.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Stop, but you cannot
change its API.

*/

package fchecker

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"net"
	"os"
)
import "time"

////////////////////////////////////////////////////// DATA
// Define the message types fchecker has to use to communicate to other
// fchecker instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fchecker instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

////////////////////////////////////////////////////// API

type StartStruct struct {
	AckLocalIPAckLocalPort       string
	EpochNonce                   uint64
	HBeatLocalIPHBeatLocalPort   string
	HBeatRemoteIPHBeatRemotePort string
	LostMsgThresh                uint8
	ServerId                     uint32
}

type HBeatMessagePayload struct {
	HBeat   HBeatMessage
	SrcAddr *net.UDPAddr
}

var (
	stopMonitored chan bool
	stopMonitor   chan bool
)

func writeMessage(msg interface{}, conn *net.UDPConn) error {
	var msgBuf bytes.Buffer
	// encode message
	encoder := gob.NewEncoder(&msgBuf)
	if encodeErr := encoder.Encode(msg); encodeErr != nil {
		log.Printf("fcheck: writeMessage: encode error: %s\n", encodeErr)
		return encodeErr
	}
	// send the message
	_, err := conn.Write(msgBuf.Bytes())
	if err != nil {
		log.Printf("fcheck: writeMessage: UDP write error: %s\n", err)
		return err
	}
	return nil
}

func MonitorRoutine(
	arg StartStruct, readAck chan AckMessage,
	notifyCh chan FailureDetected, listenConn *net.UDPConn,
) {

	// initiate connection on HBeatRemoteIPHBeatRemotePort
	localAddr, err := net.ResolveUDPAddr("udp", arg.HBeatLocalIPHBeatLocalPort)
	if err != nil {
		log.Printf("fcheck: MonitorRoutine: resolveUDPaddr error: %v\n", err)
		return
	}
	remoteAddr, err := net.ResolveUDPAddr(
		"udp", arg.HBeatRemoteIPHBeatRemotePort,
	)
	if err != nil {
		log.Printf("fcheck: MonitorRoutine: resolveUDPaddr error: %v\n", err)
		return
	}
	conn, err := net.DialUDP("udp", localAddr, remoteAddr)
	if err != nil {
		log.Printf("fcheck: MonitorRoutine: UDP dialing error: %s\n", err)
		return
	}

	//log.Printf(
	//	"fcheck: MonitorRoutine: Beginning to monitor %v from %v\n",
	//	arg.HBeatRemoteIPHBeatRemotePort, conn.LocalAddr(),
	//)
	log.Printf(
		"fcheck for server %v: MonitorRoutine: Beginning to monitor %v from"+
			" %v\n", arg.ServerId,
		conn.RemoteAddr(), conn.LocalAddr(),
	)

	lostMsgs := uint8(0)   // number of outstanding heartbeats arent acked within RTT
	rtt := 3 * time.Second // RTT in microseconds
	seqNum := uint64(0)
	sendTimes := make(map[uint64]time.Time) // map from sequence number to send time to compute RTT

	// send the first heartbeat
	sendTimes[seqNum] = time.Now()
	hbeatMsg := HBeatMessage{
		EpochNonce: arg.EpochNonce,
		SeqNum:     seqNum,
	}
	err = writeMessage(hbeatMsg, conn)
	if err != nil {
		return
	}
	log.Printf("fcheck: MonitorRoutine: Sent first heartbeat: %v\n", hbeatMsg)

	for {
		select {
		case <-stopMonitor:
			log.Println("fcheck: MonitorRoutine: stopped monitor!")
			conn.Close()
			close(readAck)
			return
		case ack := <-readAck:
			// check if this is an ack we expect
			if ack.HBEatEpochNonce != arg.EpochNonce {
				continue
			} else {
				// record receive time & update RTT
				prevRtt := rtt
				duration := time.Since(sendTimes[ack.HBEatSeqNum])
				computedRtt := duration.Seconds()
				////
				////// todo redo rtt because not working/too short
				rtt = time.Duration((float64(prevRtt) + computedRtt) / 2)

				seqNum++
				lostMsgs = 0

				// record send time of the next heartbeat
				sendTimes[seqNum] = time.Now()

				// send the heartbeat
				hbeatMsg := HBeatMessage{
					EpochNonce: arg.EpochNonce,
					SeqNum:     seqNum,
				}
				err = writeMessage(hbeatMsg, conn)
				if err != nil {
					return
				}
				//log.Printf(
				//	"fcheck: MonitorRoutine: sent heartbeat: %v\n", hbeatMsg,
				//)
			}
		default:
			// attempt to receive the ack within RTT
			// todo rtt
			rtt = (3 * time.Second)
			err := conn.SetReadDeadline(time.Now().Add(rtt))
			if err != nil {
				log.Printf(
					"fcheck: MonitorRoutine: error with SetReadDeadline: ", err,
				)
				time.Sleep(1 * time.Second)
				continue
			}
			ackMsg := make([]byte, 1024)
			n, err := conn.Read(ackMsg)
			if err != nil {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					lostMsgs++
					//log.Printf("fcheck: MonitorRoutine: timeout error: %v, lostmsgs: %v, thresh: %v, RTT: %v\n", e, lostMsgs, arg.LostMsgThresh, rtt)
					if lostMsgs >= arg.LostMsgThresh {
						failureDetected := FailureDetected{
							UDPIpPort: arg.HBeatRemoteIPHBeatRemotePort,
							Timestamp: time.Now(),
						}
						conn.Close()
						listenConn.Close()
						log.Println("fcheck: MonitorRoutine: failure detected!, closed connections!")
						close(readAck)
						notifyCh <- failureDetected
						return
					}
					continue
				} else {
					log.Printf("fcheck: MonitorRoutine: read error: %v\n", err)
					return
				}
			}
			// decode the received ack
			ackBuf := bytes.NewBuffer(ackMsg[0:n])
			var ack AckMessage
			decoder := gob.NewDecoder(ackBuf)
			if decodeErr := decoder.Decode(&ack); decodeErr != nil {
				log.Printf(
					"fcheck: MonitorRoutine: decode error: %v\n", decodeErr,
				)
				return
			}
			//log.Printf("fcheck: MonitorRoutine: received ack: %v\n", ack)
			readAck <- ack
		}
	}
}

//func sendShim()

/*
func sendShim(b []byte, conn net.Conn) (n int, e error) {
	randomNum := rand.Intn(2)
	duplicated, regular, lost := false, false, false
	if randomNum%3 == 0 {
		duplicated = true
		//lost = true
	} else if randomNum%3 == 1 {
		regular = true
	} else {
		lost = true
	}

	if duplicated {
		// write twice
		n, clientErr := conn.Write(b)
		if clientErr != nil {
			log.Printf("Client write error: %s\n", clientErr)
			return n, clientErr
		}
		n, clientErr = conn.Write(b)
		if clientErr != nil {
			log.Printf("Client write error: %s\n", clientErr)
			return n, clientErr
		}
		return n, nil
	} else if regular {
		// write once
		n, clientErr := conn.Write(b)
		if clientErr != nil {
			log.Printf("Client write error: %s\n", clientErr)
			return n, clientErr
		}
		return n, nil
	} else if lost {
		// don't write
		return 0, nil
	} else {
		log.Println("shouldn't be here!!!!!!!!!!!!!!!!!!!")
		return 0, errors.New("shouldn't be here!!!!!!!!!!!!!!!!!!!")
	}
}
*/

func MonitoredRoutine(
	readHBeat chan HBeatMessagePayload, conn *net.UDPConn, serverId uint32,
) {

	// create a log file and log to both console and terminal
	logFile, err := os.OpenFile(
		"bagel.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	log.Printf(
		"fcheck for server %v: MonitoredRoutine: starting connection"+
			" to"+
			" monitor local: %v, remote: %v\n", serverId, conn.LocalAddr(),
		conn.RemoteAddr(),
	)
	for {
		select {
		case <-stopMonitored:
			conn.Close()
			log.Println("fcheck: MonitoredRoutine: stopped monitored!")
			close(readHBeat)
			return
		case hBeatPayload := <-readHBeat:
			// create the ack
			ack := AckMessage{
				HBEatEpochNonce: hBeatPayload.HBeat.EpochNonce,
				HBEatSeqNum:     hBeatPayload.HBeat.SeqNum,
			}

			// todo remove?
			time.Sleep(2 * time.Second)

			// todo for testing, remove
			//if int(ack.HBEatSeqNum) > 3 {
			//	time.Sleep(3 * time.Second)
			//}

			// send the ack
			var msgBuf bytes.Buffer
			// encode message
			encoder := gob.NewEncoder(&msgBuf)
			if encodeErr := encoder.Encode(ack); encodeErr != nil {
				log.Printf(
					"fcheck: MonitoredRoutine: encode error: %s\n", encodeErr,
				)
				conn.Close()
				close(readHBeat)
				return
			}
			_, err := conn.WriteToUDP(msgBuf.Bytes(), hBeatPayload.SrcAddr)
			if err != nil {
				log.Printf(
					"fcheck: MonitoredRoutine: - UDP write error: %s\n", err,
				)
				conn.Close()
				close(readHBeat)
				return
			}
			//log.Printf("MonitoredRoutine: conn: %v - sent ack: %v\n", conn, ack)
		default:
			// receive the hBeat
			hBeatMsg := make([]byte, 1024)
			n, srcAddr, err := conn.ReadFromUDP(hBeatMsg)
			if err != nil {
				conn.Close()
				close(readHBeat)
				return
			}
			// decode the hBeat
			hBeatBuf := bytes.NewBuffer(hBeatMsg[0:n])
			var hBeat HBeatMessage
			decoder := gob.NewDecoder(hBeatBuf)
			if decodeErr := decoder.Decode(&hBeat); decodeErr != nil {
				conn.Close()
				close(readHBeat)
				return
			}
			// create heartbeat channel payload
			hBeatPayload := HBeatMessagePayload{
				HBeat:   hBeat,
				SrcAddr: srcAddr,
			}
			//log.Printf("MonitoredRoutine: received heartbeat: %v\n", hBeat)
			// send heartbeat to the channel
			readHBeat <- hBeatPayload
		}
	}
}

// Starts the fcheck library.

func Start(arg StartStruct) (
	notifyCh <-chan FailureDetected, addr string, err error,
) {

	// create a log file and log to both console and terminal
	logFile, err := os.OpenFile(
		"bagel.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644,
	)
	if err != nil {
		log.Print(err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	if arg.HBeatLocalIPHBeatLocalPort == "" {
		// ONLY arg.AckLocalIPAckLocalPort is set
		//
		// Start fcheck without monitoring any node, but responding to heartbeats.
		readHBeat := make(
			chan HBeatMessagePayload, 1,
		) // heartbeats received by MonitoredRoutine
		stopMonitored = make(chan bool, 1) // notify MonitoredRoutine to stop

		// attempt to listen on AckLocalIP:AckLocalPort for monitoring servers
		addr, err := net.ResolveUDPAddr("udp", arg.AckLocalIPAckLocalPort)

		if err != nil {
			log.Printf(
				"fcheck: Start: (1) Could not resolve UDP address to listen on AckLocalIP:AckLocalPort - error: %v\n",
				err,
			)
			return nil, "", err
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			log.Printf(
				"fcheck: Start: (1) Could not set up listenUDP to listen for heartbeats - error: %v\n",
				err,
			)
			conn.Close()
			return nil, "", err
		}

		// start goroutines
		go MonitoredRoutine(readHBeat, conn, arg.ServerId)

		return nil, conn.LocalAddr().String(), nil
	} else {
		// Else: ALL fields in arg are set
		// Start the fcheck library by monitoring a single node and
		// also responding to heartbeats.
		readHBeat := make(
			chan HBeatMessagePayload, 1,
		) // heartbeats received by MonitoredRoutine
		readAck := make(chan AckMessage, 1) // acks received by MonitorRoutine
		stopMonitored = make(chan bool, 1)  // notify MonitoredRoutine to stop
		stopMonitor = make(chan bool, 1)    // notify MonitorRoutine to stop
		notifyCh := make(chan FailureDetected, 1)

		// attempt to listen on AckLocalIP:AckLocalPort for monitoring servers
		addr, err := net.ResolveUDPAddr("udp", arg.AckLocalIPAckLocalPort)

		if err != nil {
			log.Printf(
				"fcheck: Start: (2) Could not set up listenUDP for AckLocalIP:AckLocalPort - error: %v\n",
				err,
			)
			return notifyCh, "", err
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			log.Printf(
				"fcheck: Start: (2) Could not listenUDP to listen for heartbeats - error: %v\n",
				err,
			)
			conn.Close()
			return notifyCh, "", err
		}

		// start goroutines
		go MonitoredRoutine(readHBeat, conn, arg.ServerId)
		go MonitorRoutine(arg, readAck, notifyCh, conn)

		return notifyCh, conn.LocalAddr().String(), nil
	}
}

// Tells the library to stop monitoring/responding acks.
func Stop() {
	stopMonitored <- true
	stopMonitor <- true
	log.Println("fcheck: Stop: Stopped monitor & monitored!")
}
