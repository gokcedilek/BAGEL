package bagel

import (
	"math"
	"reflect"
	"testing"
)

const (
	TEST_VERTEX_ID           = uint64(1)
	TEST_SUPERSTEP           = uint64(10)
	float64EqualityThreshold = 1e-8
)

func TestComputeShortestPathOneMessageShouldUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.messages = append(vertex.messages, createTestMessage(2, 3))
	vertex.neighbors = append(vertex.neighbors, 5, 6)

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
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 4)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP, 6, 4)
}

func TestComputeShortestPathOneMessageNoUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.messages = append(vertex.messages, createTestMessage(2, 100))
	vertex.neighbors = append(vertex.neighbors, 5, 6)

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
	vertex.messages = append(vertex.messages,
		createTestMessage(2, 12),
		createTestMessage(3, 2),
		createTestMessage(4, 7),
	)
	vertex.neighbors = append(vertex.neighbors, 5, 6, 7)

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
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 3)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP, 6, 3)
	assertMessageMatches(t, result[2], TEST_SUPERSTEP, 7, 3)
}

func TestComputeShortestPathMultipleMessagesNoUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.messages = append(vertex.messages,
		createTestMessage(2, 12),
		createTestMessage(3, 13),
		createTestMessage(4, 17),
	)
	vertex.neighbors = append(vertex.neighbors, 5, 6, 7)

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

func TestComputePageRankOneMessageOneNeighbor(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.messages = append(vertex.messages, createTestMessage(2, 0.5))
	vertex.neighbors = append(vertex.neighbors, 5)

	result := vertex.Compute(PAGE_RANK)
	if !almostEqual(vertex.currentValue.(float64), 0.65) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 1 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 0.65)
}

func TestComputePageRankTwoMessagesOneNeighbor(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.messages = append(vertex.messages,
		createTestMessage(2, 0.5),
		createTestMessage(3, 0.75),
	)
	vertex.neighbors = append(vertex.neighbors, 5)

	result := vertex.Compute(PAGE_RANK)
	if !almostEqual(vertex.currentValue.(float64), 1.4) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 1 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 1.4)
}

func TestComputePageRankOneMessageTwoNeighbors(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.messages = append(vertex.messages, createTestMessage(2, 0.55))
	vertex.neighbors = append(vertex.neighbors, 5, 6)

	result := vertex.Compute(PAGE_RANK)
	if !almostEqual(vertex.currentValue.(float64), 0.70) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 2 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 0.35)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP, 6, 0.35)
}

func TestComputePageRankManyMessagesManyNeighbors(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.messages = append(vertex.messages,
		createTestMessage(2, 0.55),
		createTestMessage(3, 0.3),
		createTestMessage(4, 1.5),
	)
	vertex.neighbors = append(vertex.neighbors, 5, 6, 7, 8, 9)

	result := vertex.Compute(PAGE_RANK)
	if !almostEqual(vertex.currentValue.(float64), 2.5) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 5 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 0.50)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP, 6, 0.50)
	assertMessageMatches(t, result[2], TEST_SUPERSTEP, 7, 0.50)
	assertMessageMatches(t, result[3], TEST_SUPERSTEP, 8, 0.50)
	assertMessageMatches(t, result[4], TEST_SUPERSTEP, 9, 0.50)
}

func TestComputePageRankNoResendIfWithinTolerance(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.messages = append(vertex.messages,
		createTestMessage(2, 0.55),
		createTestMessage(3, 0.3),
		createTestMessage(4, 1.5),
	)
	vertex.neighbors = append(vertex.neighbors, 5, 6, 7, 8, 9)

	result := vertex.Compute(PAGE_RANK)
	if !almostEqual(vertex.currentValue.(float64), 2.5) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 5 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 0.50)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP, 6, 0.50)
	assertMessageMatches(t, result[2], TEST_SUPERSTEP, 7, 0.50)
	assertMessageMatches(t, result[3], TEST_SUPERSTEP, 8, 0.50)
	assertMessageMatches(t, result[4], TEST_SUPERSTEP, 9, 0.50)

	vertex.messages[0].Value = vertex.messages[0].Value.(float64) + EPSILON/2 // hope the change is < EPSILON
	result = vertex.Compute(PAGE_RANK)
	if !almostEqual(vertex.currentValue.(float64), 2.5) {
		t.Errorf("vertex updated pagerank value when it should not")
	}
	if len(result) != 0 {
		t.Errorf("wrong number of outgoing messages")
	}
	if vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
}

func TestComputePageRankOnlyRecomputeNewMessages(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.messages = append(vertex.messages,
		createTestMessage(2, 0.55),
		createTestMessage(3, 0.3),
		createTestMessage(4, 1.5),
	)
	vertex.neighbors = append(vertex.neighbors, 5, 6, 7, 8, 9)

	result := vertex.Compute(PAGE_RANK)
	if !almostEqual(vertex.currentValue.(float64), 2.5) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 5 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP, 5, 0.50)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP, 6, 0.50)
	assertMessageMatches(t, result[2], TEST_SUPERSTEP, 7, 0.50)
	assertMessageMatches(t, result[3], TEST_SUPERSTEP, 8, 0.50)
	assertMessageMatches(t, result[4], TEST_SUPERSTEP, 9, 0.50)

	vertex.messages = make([]Message, 1)
	vertex.SuperStep = vertex.SuperStep + 1
	vertex.messages[0] = createTestMessage(3, 0.8)

	result = vertex.Compute(PAGE_RANK)
	if !almostEqual(vertex.currentValue.(float64), 3.0) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 5 {
		t.Errorf("wrong number of outgoing messages")
	}
	if !vertex.isActive {
		t.Errorf("vertex did not update isActive correctly")
	}
	assertMessageMatches(t, result[0], TEST_SUPERSTEP+1, 5, 0.60)
	assertMessageMatches(t, result[1], TEST_SUPERSTEP+1, 6, 0.60)
	assertMessageMatches(t, result[2], TEST_SUPERSTEP+1, 7, 0.60)
	assertMessageMatches(t, result[3], TEST_SUPERSTEP+1, 8, 0.60)
	assertMessageMatches(t, result[4], TEST_SUPERSTEP+1, 9, 0.60)

}

func createNewTestVertex(initialVal interface{}) Vertex {
	vertex := Vertex{
		Id:             TEST_VERTEX_ID,
		neighbors:      make([]uint64, 0),
		previousValues: make(map[uint64]interface{}),
		currentValue:   initialVal,
		messages:       make([]Message, 0),
		isActive:       true,
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
	superStepNum uint64, destVertexId uint64, value interface{}) {

	if message.SuperStepNum != superStepNum {
		t.Errorf("incorrect super step number: expected %v but got %v", superStepNum, message.SuperStepNum)
	}

	if message.SourceVertexId != TEST_VERTEX_ID {
		t.Errorf("incorrect source vertex id: expected %v but got %v", TEST_VERTEX_ID, message.SourceVertexId)
	}

	if message.DestVertexId != destVertexId {
		t.Errorf("incorrect destination vertex id: expected %v but got %v", destVertexId, message.DestVertexId)
	}

	if reflect.TypeOf(message.Value) != reflect.TypeOf(value) {
		t.Errorf("value and message have different types: expected %T but got %T", value, message.Value)
	}

	switch messageValue := message.Value.(type) {
	case uint64:
		if messageValue != value {
			t.Errorf("int message has incorrect value: expected %v but got %v", value, messageValue)
		}
	case float64:
		if !almostEqual(messageValue, value.(float64)) {
			t.Errorf("float64 message has incorrect value: expected %v but got %v", value, messageValue)
		}
	}

}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= float64EqualityThreshold
}
