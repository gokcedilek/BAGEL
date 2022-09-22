package database

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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

func GetVertexByID(svc *dynamodb.Client, vertexId int64, tableName string) (Vertex, error) {
	if svc == nil {
		svc = GetDynamoClient()
	}

	res, err := svc.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberN{Value: strconv.FormatInt(vertexId, 10)},
		},
	})

	if err != nil {
		return Vertex{}, err
	}

	vertex := Vertex{}
	attributevalue.UnmarshalMap(res.Item, &vertex)
	return vertex, nil
}

func GetPartition(svc *dynamodb.Client, tableName string, partitionNum int) ([]Vertex, error) {
	return nil, nil // todo
}

func PartitionGraph(svc *dynamodb.Client, tableName string, numPartition int) {
	GetAllVertices(svc, tableName)
}

func GetAllVertices(svc *dynamodb.Client, tableName string) []Vertex {
	p := dynamodb.NewScanPaginator(svc, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	fmt.Println(p)
	return nil
}

func IsPartitionCached(svc *dynamodb.Client, tableName string, numPartition int) bool {
	limit := int32(1)
	partitionName := getNumberOfWorkersXName(numPartition)

	out, err := svc.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		FilterExpression: aws.String(fmt.Sprintf("attribute_exists(%s)", partitionName)),
		Limit:            &limit,
	})

	if err != nil {
		log.Fatalf("Failed to verify partition cache %v\n", err)
	}

	return len(out.Items) != 0
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

		log.Printf("Successfully uploaded batch %v/%v\n", b+1, numBatches)
	}

	log.Printf("%v batches added to %v", numBatches, tableName)
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

func getNumberOfWorkersXName(x int) string {
	return fmt.Sprintf("P%d", x)
}
