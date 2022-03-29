package util

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
)

func ReadJSONConfig(filename string, config interface{}) error {
	configData, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	err = json.Unmarshal(configData, config)
	if err != nil {
		return err
	}
	return nil
}

func CheckErr(err error, errfmsg string, fargs ...interface{}) {
	if err != nil {
		fmt.Fprintf(os.Stderr, errfmsg, fargs...)
		os.Exit(1)
	}
}

func DialTCPCustom(localAddr string, remoteAddr string) (*net.TCPConn, error) {
	var laddr *net.TCPAddr = nil
	var err error

	if localAddr != "" {
		laddr, err = net.ResolveTCPAddr("tcp", localAddr)
		CheckErr(err, "could not resolve local address: %v", localAddr)
	}

	raddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	CheckErr(err, "could not resolve remote address: %v", remoteAddr)

	conn, err := net.DialTCP("tcp", laddr, raddr)

	if err != nil {
		return nil, err
	}

	// TODO !!! remove for final submission
	err = conn.SetLinger(0)
	return conn, err
}
