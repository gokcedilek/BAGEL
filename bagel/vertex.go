package bagel

// Vertex stores intermediate calculation data about the vertex
type Vertex struct {
	Id           uint64
	neighbors    []NeighbourVertex
	currentValue float64
	messages     []Message
	isActive     bool
	workerAddr   string
	Superstep    uint64
}

type VertexCheckpoint struct {
	CurrentValue float64
	Messages     []Message
	IsActive     bool
}

type NeighbourVertex struct {
	vertexId uint64
}

type VertexPair struct {
	srcId  uint64
	destId uint64
}

func NewVertex() *Vertex {
	return &Vertex{}
}

type ShortestPathVertex Vertex
type PageRankVertex Vertex

func (v *Vertex) Compute() []Message {
	return nil // stub
}
