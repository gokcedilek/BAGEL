package database

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"math"
	"strconv"
)

const CENTRAL_DB_NAME = "bagel-db"
const DEFAULT_REGION = "us-east-2"
const MAXIMUM_ITEMS_PER_BATCH = 25

type Graph map[uint64][]uint64

type Vertex struct {
	ID    uint64
	Edges []uint64
	Hash  int64
}

func GetDynamoClient() *dynamodb.Client {
	config, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(DEFAULT_REGION))

	if err != nil {
		log.Fatalf("unable to load SDK config %v", err)
	}

	return dynamodb.NewFromConfig(config)
}

func InsertVertex(svc *dynamodb.Client, tableName string, vertex Vertex) {
	if svc == nil {
		svc = GetDynamoClient()
	}

	db_vertex := marshalVertex(tableName, vertex)
	out, err := svc.PutItem(context.TODO(), &db_vertex)

	if err != nil {
		log.Fatalf("Failed to insert vertex %v\n", vertex.ID, err)
	}

	fmt.Println(out.Attributes)
}

func BatchInsertVertices(svc *dynamodb.Client, tableName string, vertices []Vertex) {
	batches, numBatches := getBatches(tableName, vertices)

	for b := 0; b < numBatches; b++ {
		_, err := svc.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: batches[b],
			},
		})

		if err != nil {
			log.Fatalf("Failed to upload batch %v. Here's why %v\n", b, err)
		}

		log.Printf("Successfully uploaded batch %v/%v\n", b, numBatches)
	}
}

func getBatches(tableName string, vertices []Vertex) ([][]types.WriteRequest, int) {
	numBatches := int(math.Ceil(float64(len(vertices)) / float64(MAXIMUM_ITEMS_PER_BATCH)))
	lastBatchSize := len(vertices) % MAXIMUM_ITEMS_PER_BATCH

	batches := make([][]types.WriteRequest, numBatches)
	for b := 0; b < numBatches; b++ {
		if b == numBatches-1 {
			batches[b] = make([]types.WriteRequest, lastBatchSize)
			continue
		}

		batches[b] = make([]types.WriteRequest, MAXIMUM_ITEMS_PER_BATCH)
	}

	for idx, vertex := range vertices {
		b := idx / MAXIMUM_ITEMS_PER_BATCH
		v := idx % MAXIMUM_ITEMS_PER_BATCH
		batches[b][v] = marshalVertexWriteReq(vertex)
	}

	return batches, numBatches
}

func marshalVertex(tableName string, vertex Vertex) dynamodb.PutItemInput {
	return dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"ID":    &types.AttributeValueMemberN{Value: strconv.FormatUint(vertex.ID, 10)},
			"Edges": &types.AttributeValueMemberL{Value: edgesToAttributeValueSlice(vertex.Edges)},
			"Hash":  &types.AttributeValueMemberN{Value: strconv.FormatInt(vertex.Hash, 10)},
		},
	}
}

func marshalVertexWriteReq(vertex Vertex) types.WriteRequest {
	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: map[string]types.AttributeValue{
				"ID":    &types.AttributeValueMemberN{Value: strconv.FormatUint(vertex.ID, 10)},
				"Edges": &types.AttributeValueMemberL{Value: edgesToAttributeValueSlice(vertex.Edges)},
				"Hash":  &types.AttributeValueMemberN{Value: strconv.FormatInt(vertex.Hash, 10)},
			},
		},
	}
}

func edgesToAttributeValueSlice(edges []uint64) []types.AttributeValue {
	as := make([]types.AttributeValue, len(edges))
	for idx, edge := range edges {
		as[idx] = &types.AttributeValueMemberN{Value: strconv.FormatUint(edge, 10)}
	}
	return as
}
