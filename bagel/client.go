package bagel

import (
	"errors"
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

	log.Printf("SendQuery: query is queued up to be sent.")
	go c.doQuery(query)
	return nil
}

func (c *GraphClient) doQuery(query Query) {
	var result QueryResult
	// TODO: implement timeout? or retry behavior? or stop handling?
	err := c.coordClient.Call("Coord.StartQuery", query, &result)
	if err != nil {
		log.Printf(
			"doQuery: error calling Coord.StartQuery:  %v\n",
			err,
		)
	}

	if result.Error != "" {
		log.Printf("doQuery: received error: %v\n", result.Error)
	}

	log.Printf("doQuery: received result: %v\n", result.Result)

	c.notifyCh <- result
}

// Start Currently based on kvslib from A3
// If there is an issue with connecting to the coord, this should return an appropriate err value
// otherwise err should be set to nil
func (c *GraphClient) Start(
	clientId string, coordAddr string, clientAddr string,
) (chan QueryResult, error) {

	// set up client state
	c.clientId = clientId

	var err error
	// connect to the coord node for RPCs
	lAddr := util.IPEmptyPortOnly(clientAddr)
	c.coordConn, err = util.DialTCPCustom(lAddr, coordAddr)
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
