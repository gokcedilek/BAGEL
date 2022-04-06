package bagel

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"project/util"
)

type ClientConfig struct {
	ClientId   string
	CoordAddr  string
	ClientAddr string
}

type GraphClient struct {
	clientId    string
	coordClient *rpc.Client
	coordConn   *net.TCPConn
	notifyCh    chan QueryResult
}

type Query struct {
	ClientId  string
	QueryType string   // PageRank or ShortestPath
	Nodes     []uint64 // if PageRank, will have 1 vertex, if shortestpath, will have [start, end]
	Graph     string   // graph to use - will always be google for now
}

type QueryResult struct {
	Query  Query
	Result interface{} // client dynamically casts Result based on Query.QueryType:
	// float64 for pagerank, int for shortest path
}

func NewClient() *GraphClient {
	return &GraphClient{}
}

func (c *GraphClient) SendQuery(query Query) error {
	switch query.QueryType {
	case PAGE_RANK:
		if len(query.Nodes) != 1 {
			return errors.New("incorrect number of vertices in the query")
		}
	case SHORTEST_PATH:
		if len(query.Nodes) != 2 {
			return errors.New("incorrect number of vertices in the query")
		}
	default:
		return errors.New("unknown query type")
	}

	log.Printf("Client: SendQuery: query is queued up to be sent.")
	go c.doQuery(query)
	return nil
}

func (c *GraphClient) doQuery(query Query) {
	var result QueryResult
	// TODO: implement timeout? or retry behavior? or stop handling?
	err := c.coordClient.Call("Coord.StartQuery", query, &result)
	if err != nil {
		log.Printf("Client: sendQuery: error calling Coord.DoQuery:  %v\n", err)
	}

	fmt.Printf("client received result: %v\n", result)
	c.notifyCh <- result
}

// Start Currently based on kvslib from A3
// If there is an issue with connecting to the coord, this should return an appropriate err value
// otherwise err should be set to nil
func (c *GraphClient) Start(
	clientId string, coordAddr string, clientAddr string,
) (chan QueryResult, error) {
	log.Printf("Client: Start\n")

	// set up client state
	c.clientId = clientId

	var err error
	// connect to the coord node for RPCs
	c.coordConn, err = util.DialTCPCustom(clientAddr, coordAddr)
	if err != nil {
		return nil, err
	}

	c.coordClient = rpc.NewClient(c.coordConn)

	c.notifyCh = make(chan QueryResult, 1)

	return c.notifyCh, nil
}

func (c *GraphClient) Stop() {
	c.coordClient.Close()
	c.coordConn.Close()
	close(c.notifyCh)
}
