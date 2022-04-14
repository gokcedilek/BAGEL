package bagel

import (
	"testing"
)

const (
	TEST_VERTEX_ID = uint64(1)
	TEST_SUPERSTEP = uint64(10)
)

func TestComputeShortestPathOneMessageShouldUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.messages = append(vertex.messages, createTestMessage(2, 3))
	vertex.neighbors = append(vertex.neighbors, NeighbourVertex{5}, NeighbourVertex{6})

	result := vertex.Compute(SHORTEST_PATH)
	if vertex.currentValue != 3 {
		t.Errorf("vertex did not update shortest path value correctly")
	}
	if len(result) != 2 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 0, 4)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP, 6, 0, 4)
}

func TestComputeShortestPathOneMessageNoUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.messages = append(vertex.messages, createTestMessage(2, 100))
	vertex.neighbors = append(vertex.neighbors, NeighbourVertex{5}, NeighbourVertex{6})

	result := vertex.Compute(SHORTEST_PATH)
	if vertex.currentValue != 10 {
		t.Errorf("vertex updated shortest path value when it should not")
	}
	if len(result) != 0 {
		t.Errorf("wrong number of outgoing messages")
	}
	if vertex.isActive {
		t.Errorf("vertex updated isActive when it should not")
	}
}

func TestComputeShortestPathMultipleMessagesShouldUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.messages = append(vertex.messages, createTestMessage(2, 12), createTestMessage(3, 2), createTestMessage(4, 7))
	vertex.neighbors = append(vertex.neighbors, NeighbourVertex{5}, NeighbourVertex{6}, NeighbourVertex{7})

	result := vertex.Compute(SHORTEST_PATH)
	if vertex.currentValue != 2 {
		t.Errorf("vertex did not update shortest path value correctly")
	}
	if len(result) != 3 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 0, 3)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP, 6, 0, 3)
	assertMessageMatches(t, result[2], TEST_SUPERSTEP, 7, 0, 3)
}

func TestComputeShortestPathMultipleMessagesNoUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.messages = append(vertex.messages, createTestMessage(2, 12), createTestMessage(3, 13), createTestMessage(4, 17))
	vertex.neighbors = append(vertex.neighbors, NeighbourVertex{5}, NeighbourVertex{6}, NeighbourVertex{7})

	result := vertex.Compute(SHORTEST_PATH)
	if vertex.currentValue != 10 {
		t.Errorf("vertex updated shortest path value when it should not")
	}
	if len(result) != 0 {
		t.Errorf("wrong number of outgoing messages")
	}
	if vertex.isActive {
		t.Errorf("vertex updated isActive when it should not")
	}
}

func createNewTestVertex(initialVal interface{}) Vertex {
	vertex := Vertex{
		Id:             TEST_VERTEX_ID,
		neighbors:      make([]NeighbourVertex, 0),
		previousValues: make(map[uint64]interface{}),
		currentValue:   initialVal,
		messages:       make([]Message, 0),
		isActive:       true,
		workerAddr:     "",
		SuperStep:      TEST_SUPERSTEP,
	}
	return vertex
}

func createTestMessage(source uint64, value interface{}) Message {
	return Message{
		SuperStepNum:   TEST_SUPERSTEP - 1,
		SourceVertexId: source,
		DestVertexId:   TEST_VERTEX_ID,
		Value:          value,
	}
}

func assertMessageMatches(t *testing.T, message Message,
	superStepNum uint64, destVertexId uint64, destHash uint64, value interface{}) {

	if message.SuperStepNum != superStepNum {
		t.Errorf("message has incorrect super step number")
	}

	if message.SourceVertexId != TEST_VERTEX_ID {
		t.Errorf("message has incorrect source vertex id")
	}

	if message.DestVertexId != destVertexId {
		t.Errorf("message has incorrect destination vertex id")
	}

	if message.Value != value {
		t.Errorf("message has incorrect value")
	}

}
