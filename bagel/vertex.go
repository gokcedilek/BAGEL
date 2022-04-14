package bagel

import "math"

// Vertex stores intermediate calculation data about the vertex
type Vertex struct {
	Id           uint64
	neighbors    []NeighbourVertex
	currentValue interface{}
	messages     []Message
	isActive     bool
	workerAddr   string
	SuperStep    uint64
}

type VertexCheckpoint struct {
	CurrentValue interface{}
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

func (v *Vertex) Compute(queryType string) []Message {
	var result []Message
	switch queryType {
	case PAGE_RANK:
		result = ComputePageRank(v)
	case SHORTEST_PATH:
		result = ComputeShortestPath(v)
	}
	v.isActive = len(result) > 0
	return result
}

func ComputePageRank(v *Vertex) []Message {
	result := make([]Message, 0)
	return result
}

func ComputeShortestPath(v *Vertex) []Message {
	result := make([]Message, 0)
	shortestNewPath := math.MaxInt64
	for _, message := range v.messages {
		pathLength := message.Value.(int) // cast to an int
		if pathLength < shortestNewPath {
			shortestNewPath = pathLength
		}
	}

	if shortestNewPath < v.currentValue.(int) {
		v.currentValue = shortestNewPath
		for _, neighborVertex := range v.neighbors {
			newMessage := Message{
				SuperStepNum:   v.SuperStep,
				SourceVertexId: v.Id,
				DestVertexId:   neighborVertex.vertexId,
				DestHash:       0, // TODO: compute the hash or store it?
				Value:          shortestNewPath + 1,
			}
			result = append(result, newMessage)
		}
	}
	return result
}
