package bagel

import "math"

const (
	EPSILON = 1e-3
)

// Vertex stores intermediate calculation data about the vertex
type Vertex struct {
	Id             uint64
	neighbors      []uint64
	previousValues map[uint64]interface{}
	currentValue   interface{}
	messages       []Message
	isActive       bool
	SuperStep      uint64
}

type VertexCheckpoint struct {
	CurrentValue interface{}
	Messages     []Message
	IsActive     bool
}

type VertexPair struct {
	srcId  uint64
	destId uint64
}

func NewVertex(id uint64, neighbors []uint64) *Vertex {
	return &Vertex{
		Id:             id,
		neighbors:      neighbors,
		previousValues: make(map[uint64]interface{}),
		messages:       make([]Message, 10),
		isActive:       false,
		SuperStep:      0,
	}
}

func NewPageRankVertex(id uint64, neighbors []uint64) *Vertex {
	prVertex := NewVertex(id, neighbors)
	prVertex.currentValue = 1
	return prVertex
}

func NewShortestPathVertex(id uint64, neighbors []uint64, value uint64) *Vertex {
	spVertex := NewVertex(id, neighbors)
	spVertex.currentValue = value
	return spVertex
}

type ShortestPathVertex Vertex
type PageRankVertex Vertex

func (v *Vertex) Compute(queryType string) []Message {
	var result []Message
	switch queryType {
	case PAGE_RANK:
		result = v.ComputePageRank()
	case SHORTEST_PATH:
		result = v.ComputeShortestPath()
	}
	v.isActive = len(result) > 0
	return result
}

func (v *Vertex) ComputeShortestPath() []Message {
	result := make([]Message, 0)
	shortestNewPath := math.MaxInt64
	for _, message := range v.messages {
		pathLength := message.Value.(int) // cast to an int
		v.previousValues[message.SourceVertexId] = pathLength
		if pathLength < shortestNewPath {
			shortestNewPath = pathLength
		}
	}

	if shortestNewPath < v.currentValue.(int) {
		v.currentValue = shortestNewPath
		for _, neighborVertexId := range v.neighbors {
			newMessage := Message{
				SuperStepNum:   v.SuperStep,
				SourceVertexId: v.Id,
				DestVertexId:   neighborVertexId,
				Value:          shortestNewPath + 1,
			}
			result = append(result, newMessage)
		}
	}
	return result
}

func (v *Vertex) ComputePageRank() []Message {
	// update flow values
	for _, message := range v.messages {
		flowValue := message.Value.(float64) // cast to an int
		v.previousValues[message.SourceVertexId] = flowValue
	}

	// calculate new value
	totalFlow := 0.15
	for _, flowValue := range v.previousValues {
		totalFlow += flowValue.(float64)
	}

	// update neighbors at next step if the change is large enough
	result := make([]Message, 0)
	if math.Abs(totalFlow-v.currentValue.(float64)) > EPSILON {
		for _, neighborVertexId := range v.neighbors {
			newMessage := Message{
				SuperStepNum:   v.SuperStep,
				SourceVertexId: v.Id,
				DestVertexId:   neighborVertexId,
				Value:          totalFlow / float64(len(v.neighbors)),
			}
			result = append(result, newMessage)
		}
		v.currentValue = totalFlow
	}
	return result
}
