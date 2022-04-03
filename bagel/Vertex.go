package bagel

// Vertex stores intermediate calculation data about the vertex
type Vertex struct {
	neighbors    []Vertex
	currentValue float64
	messages     []Message
	isActive     bool
	workerAddr   string
	Superstep    uint64
}

func NewVertex() *Vertex {
	return &Vertex{}
}

type ShortestPathVertex Vertex
type PageRankVertex Vertex

func (v *Vertex) Compute() map[uint32]uint64 {
	return nil // stub
}
