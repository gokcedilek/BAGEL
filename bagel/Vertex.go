package bagel

// Vertex stores intermediate calculation data about the vertex
type Vertex struct {
	neighbors    []Vertex
	currentValue float64
	messages     []Message
	isActive     bool
	workerAddr   string
}
