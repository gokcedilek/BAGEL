package worker

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
