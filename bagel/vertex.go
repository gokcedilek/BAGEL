package bagel

import (
	"log"
	"math"
)

const (
	EPSILON               = 0.1
	MAX_ITERATIONS        = 50
	INITIALIZATION_VERTEX = math.MaxUint64
)

// Vertex stores intermediate calculation data about the vertex
type Vertex struct {
	Id             uint64
	Neighbors      []uint64
	PreviousValues map[uint64]interface{}
	CurrentValue   interface{}
	Messages       []Message
	IsActive       bool
}

type VertexCheckpoint Vertex

func NewVertex(id uint64, neighbors []uint64) *Vertex {
	return &Vertex{
		Id:             id,
		Neighbors:      neighbors,
		PreviousValues: make(map[uint64]interface{}),
		Messages:       make([]Message, 0),
		IsActive:       false,
	}
}

func NewPageRankVertex(id uint64, neighbors []uint64) *Vertex {
	prVertex := NewVertex(id, neighbors)
	prVertex.CurrentValue = float64(0)
	return prVertex
}

func NewShortestPathVertex(id uint64, neighbors []uint64, value int) *Vertex {
	spVertex := NewVertex(id, neighbors)
	spVertex.CurrentValue = value
	return spVertex
}

func (v *Vertex) SetSuperStepInfo(messages []Message) {
	v.Messages = messages
}

func (v *Vertex) Compute(queryType string, isRestart bool) []Message {
	var result []Message
	switch queryType {
	case PAGE_RANK:
		result = v.ComputePageRank()
	case SHORTEST_PATH:
		result = v.ComputeShortestPath(isRestart)
	}
	v.IsActive = len(result) > 0
	return result
}

func (v *Vertex) ComputeShortestPath(isRestart bool) []Message {
	result := make([]Message, 0)
	shortestNewPath := math.MaxInt32
	for _, message := range v.Messages {
		pathLength := message.Value.(int) // cast to an int
		v.PreviousValues[message.SourceVertexId] = pathLength
		if pathLength < shortestNewPath {
			shortestNewPath = pathLength
		}
	}

	if (shortestNewPath < v.CurrentValue.(int)) || (isRestart && v.CurrentValue.(int) < math.MaxInt32) {
		// if (shortestNewPath < v.CurrentValue.(int)) {
		v.CurrentValue = shortestNewPath
		for _, neighborVertexId := range v.Neighbors {
			newMessage := Message{
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
	for _, message := range v.Messages {
		flowValue := message.Value.(float64) // cast to an int
		if message.SourceVertexId == INITIALIZATION_VERTEX {
			totalFlow += flowValue
		} else {
			v.PreviousValues[message.SourceVertexId] = flowValue
		}
	}

	// calculate new value
	for _, flowValue := range v.PreviousValues {
		totalFlow += flowValue.(float64)
	}

	// update neighbors at next step if the change is large enough
	result := make([]Message, 0)
	if math.Abs(totalFlow-v.CurrentValue.(float64)) > EPSILON {
		for _, neighborVertexId := range v.Neighbors {
			newMessage := Message{
				SourceVertexId: v.Id,
				DestVertexId:   neighborVertexId,
				Value:          totalFlow / float64(len(v.Neighbors)),
			}
			result = append(result, newMessage)
		}
		v.CurrentValue = totalFlow
	}
	return result
}

func IsTargetVertex(
	vertexId uint64, vertices []uint64, vertexType string,
) bool {
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
