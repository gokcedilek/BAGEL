package util

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
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

func WriteJSONConfig(filename string, config interface{}) error {
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0666)
}

func CheckErr(err error, errfmsg string, fargs ...interface{}) {
	if err != nil {
		fmt.Fprintf(os.Stderr, errfmsg, fargs...)
		log.Fatalf(errfmsg, fargs...)
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

	err = conn.SetLinger(0)
	return conn, err
}

func DialRPC(address string) (*rpc.Client, error) {
	conn, err := DialTCPCustom("", address)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), err
}

func GetProjectRoot() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(b))
}
