package bagel

import (
	"math"
	"reflect"
	"testing"
)

const (
	TEST_VERTEX_ID           = uint64(1)
	float64EqualityThreshold = 1e-8
)

func TestComputeShortestPathOneMessageshouldUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.Messages = append(vertex.Messages, createTestMessage(2, 3))
	vertex.Neighbors = append(vertex.Neighbors, 5, 6)

	result := vertex.Compute(SHORTEST_PATH, false)
	if vertex.CurrentValue != 3 {
		t.Errorf("vertex did not update shortest path value correctly")
	}
	if len(result) != 2 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 4)
	assertMessageMatches(t, result[1], 6, 4)
}

func TestComputeShortestPathOneMessageNoUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.Messages = append(vertex.Messages, createTestMessage(2, 100))
	vertex.Neighbors = append(vertex.Neighbors, 5, 6)

	result := vertex.Compute(SHORTEST_PATH, false)
	if vertex.CurrentValue != 10 {
		t.Errorf("vertex updated shortest path value when it should not")
	}
	if len(result) != 0 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if vertex.IsActive {
		t.Errorf("vertex updated IsActive when it should not")
	}
}

func TestComputeShortestPathMultipleMessagesShouldUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.Messages = append(vertex.Messages,
		createTestMessage(2, 12),
		createTestMessage(3, 2),
		createTestMessage(4, 7),
	)
	vertex.Neighbors = append(vertex.Neighbors, 5, 6, 7)

	result := vertex.Compute(SHORTEST_PATH, false)
	if vertex.CurrentValue != 2 {
		t.Errorf("vertex did not update shortest path value correctly")
	}
	if len(result) != 3 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 3)
	assertMessageMatches(t, result[1], 6, 3)
	assertMessageMatches(t, result[2], 7, 3)
}

func TestComputeShortestPathMultipleMessagesNoUpdate(t *testing.T) {
	vertex := createNewTestVertex(10)
	vertex.Messages = append(vertex.Messages,
		createTestMessage(2, 12),
		createTestMessage(3, 13),
		createTestMessage(4, 17),
	)
	vertex.Neighbors = append(vertex.Neighbors, 5, 6, 7)

	result := vertex.Compute(SHORTEST_PATH, false)
	if vertex.CurrentValue != 10 {
		t.Errorf("vertex updated shortest path value when it should not")
	}
	if len(result) != 0 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if vertex.IsActive {
		t.Errorf("vertex updated IsActive when it should not")
	}
}

func TestComputePageRankOneMessageOneNeighbor(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.Messages = append(vertex.Messages, createTestMessage(2, 0.5))
	vertex.Neighbors = append(vertex.Neighbors, 5)

	result := vertex.Compute(PAGE_RANK, false)
	if !almostEqual(vertex.CurrentValue.(float64), 0.65) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 1 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 0.65)
}

func TestComputePageRankTwoMessagesOneNeighbor(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.Messages = append(vertex.Messages,
		createTestMessage(2, 0.5),
		createTestMessage(3, 0.75),
	)
	vertex.Neighbors = append(vertex.Neighbors, 5)

	result := vertex.Compute(PAGE_RANK, false)
	if !almostEqual(vertex.CurrentValue.(float64), 1.4) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 1 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 1.4)
}

func TestComputePageRankOneMessageTwoNeighbors(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.Messages = append(vertex.Messages, createTestMessage(2, 0.55))
	vertex.Neighbors = append(vertex.Neighbors, 5, 6)

	result := vertex.Compute(PAGE_RANK, false)
	if !almostEqual(vertex.CurrentValue.(float64), 0.70) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 2 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 0.35)
	assertMessageMatches(t, result[1], 6, 0.35)
}

func TestComputePageRankManyMessagesManyNeighbors(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.Messages = append(vertex.Messages,
		createTestMessage(2, 0.55),
		createTestMessage(3, 0.3),
		createTestMessage(4, 1.5),
	)
	vertex.Neighbors = append(vertex.Neighbors, 5, 6, 7, 8, 9)

	result := vertex.Compute(PAGE_RANK, false)
	if !almostEqual(vertex.CurrentValue.(float64), 2.5) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 5 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 0.50)
	assertMessageMatches(t, result[1], 6, 0.50)
	assertMessageMatches(t, result[2], 7, 0.50)
	assertMessageMatches(t, result[3], 8, 0.50)
	assertMessageMatches(t, result[4], 9, 0.50)
}

func TestComputePageRankNoResendIfWithinTolerance(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.Messages = append(vertex.Messages,
		createTestMessage(2, 0.55),
		createTestMessage(3, 0.3),
		createTestMessage(4, 1.5),
	)
	vertex.Neighbors = append(vertex.Neighbors, 5, 6, 7, 8, 9)

	result := vertex.Compute(PAGE_RANK, false)
	if !almostEqual(vertex.CurrentValue.(float64), 2.5) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 5 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 0.50)
	assertMessageMatches(t, result[1], 6, 0.50)
	assertMessageMatches(t, result[2], 7, 0.50)
	assertMessageMatches(t, result[3], 8, 0.50)
	assertMessageMatches(t, result[4], 9, 0.50)

	vertex.Messages[0].Value = vertex.Messages[0].Value.(float64) + EPSILON/2 // hope the change is < EPSILON
	result = vertex.Compute(PAGE_RANK, false)
	if !almostEqual(vertex.CurrentValue.(float64), 2.5) {
		t.Errorf("vertex updated pagerank value when it should not")
	}
	if len(result) != 0 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
}

func TestComputePageRankOnlyRecomputeNewMessages(t *testing.T) {
	vertex := createNewTestVertex(1.0)
	vertex.Messages = append(vertex.Messages,
		createTestMessage(2, 0.55),
		createTestMessage(3, 0.3),
		createTestMessage(4, 1.5),
	)
	vertex.Neighbors = append(vertex.Neighbors, 5, 6, 7, 8, 9)

	result := vertex.Compute(PAGE_RANK, false)
	if !almostEqual(vertex.CurrentValue.(float64), 2.5) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 5 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 0.50)
	assertMessageMatches(t, result[1], 6, 0.50)
	assertMessageMatches(t, result[2], 7, 0.50)
	assertMessageMatches(t, result[3], 8, 0.50)
	assertMessageMatches(t, result[4], 9, 0.50)

	vertex.Messages = make([]Message, 1)
	vertex.Messages[0] = createTestMessage(3, 0.8)

	result = vertex.Compute(PAGE_RANK, false)
	if !almostEqual(vertex.CurrentValue.(float64), 3.0) {
		t.Errorf("vertex did not update pagerank value correctly")
	}
	if len(result) != 5 {
		t.Errorf("wrong number of outgoing Messages")
	}
	if !vertex.IsActive {
		t.Errorf("vertex did not update IsActive correctly")
	}
	assertMessageMatches(t, result[0], 5, 0.60)
	assertMessageMatches(t, result[1], 6, 0.60)
	assertMessageMatches(t, result[2], 7, 0.60)
	assertMessageMatches(t, result[3], 8, 0.60)
	assertMessageMatches(t, result[4], 9, 0.60)

}

func createNewTestVertex(initialVal interface{}) Vertex {
	vertex := Vertex{
		Id:             TEST_VERTEX_ID,
		Neighbors:      make([]uint64, 0),
		PreviousValues: make(map[uint64]interface{}),
		CurrentValue:   initialVal,
		Messages:       make([]Message, 0),
		IsActive:       true,
	}
	return vertex
}

func createTestMessage(source uint64, value interface{}) Message {
	return Message{
		SourceVertexId: source,
		DestVertexId:   TEST_VERTEX_ID,
		Value:          value,
	}
}

func assertMessageMatches(t *testing.T, message Message, destVertexId uint64, value interface{}) {

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
