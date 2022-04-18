package bagel

import (
	"log"
	"math"
)

const (
	EPSILON               = 0.05
	MAX_ITERATIONS        = 500
	INITIALIZATION_VERTEX = math.MaxUint64
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

func NewVertex(id uint64, neighbors []uint64) *Vertex {
	return &Vertex{
		Id:             id,
		neighbors:      neighbors,
		previousValues: make(map[uint64]interface{}),
		messages:       make([]Message, 0),
		isActive:       false,
		SuperStep:      0,
	}
}

func NewPageRankVertex(id uint64, neighbors []uint64) *Vertex {
	prVertex := NewVertex(id, neighbors)
	prVertex.currentValue = float64(0)
	return prVertex
}

func NewShortestPathVertex(id uint64, neighbors []uint64, value int) *Vertex {
	spVertex := NewVertex(id, neighbors)
	spVertex.currentValue = value
	return spVertex
}

func (v *Vertex) SetSuperStepInfo(superStepNum uint64, messages []Message) {
	v.SuperStep = superStepNum
	v.messages = messages
}

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
	shortestNewPath := math.MaxInt32
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
	totalFlow := 0.15

	// update flow values
	for _, message := range v.messages {
		flowValue := message.Value.(float64) // cast to an int
		if message.SourceVertexId == INITIALIZATION_VERTEX {
			totalFlow += flowValue
		} else {
			v.previousValues[message.SourceVertexId] = flowValue
		}
	}

	// calculate new value
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

func IsTargetVertex(vertexId uint64, vertices []uint64, vertexType string) bool {
	switch vertexType {
	case SHORTEST_PATH_SOURCE:
		return isSourceSPVertex(vertexId, vertices)
	case SHORTEST_PATH_DEST:
		return isTargetSPVertex(vertexId, vertices)
	case PAGE_RANK:
		return isTargetPRVertex(vertexId, vertices)
	default:
		log.Println("WARNING - isTargetVertex: query for unknown vertex type")
		return false
	}
}

func isTargetSPVertex(vertexId uint64, vertices []uint64) bool {
	if len(vertices) < 2 {
		return false
	}

	return vertexId == vertices[1]
}

func isSourceSPVertex(vertexId uint64, vertices []uint64) bool {
	if len(vertices) < 2 {
		return false
	}
	return vertexId == vertices[0]
}

func isTargetPRVertex(vertexId uint64, vertices []uint64) bool {
	if len(vertices) < 1 {
		return false
	}

	return vertexId == vertices[0]
}
