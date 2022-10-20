package database

import (
	"bufio"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"os"
	"project/util"
	"strconv"
	"strings"
	"time"
)

func createTable(context context.Context, svc *dynamodb.Client, tableDefinition *dynamodb.CreateTableInput) {
	out, err := svc.CreateTable(context, tableDefinition)
	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully created database", out)
}

func CreateTable(context context.Context, svc *dynamodb.Client, tableName string) {
	bagelDefinition := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       types.KeyTypeHash,
			},
		},
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
	}

	createTable(context, svc, bagelDefinition)
	waitForTable(context, svc, CENTRAL_DB_NAME)
}

func AddGraph(svc *dynamodb.Client, filePath string, tableName string) {
	vertices := parseInputGraph(filePath)
	batches := CreateBatches(vertices)
	BatchInsertVertices(svc, tableName, batches)
}

func waitForTable(ctx context.Context, db *dynamodb.Client, tn string) error {
	w := dynamodb.NewTableExistsWaiter(db)
	err := w.Wait(ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(tn),
		},
		2*time.Minute,
		func(o *dynamodb.TableExistsWaiterOptions) {
			o.MaxDelay = 5 * time.Second
			o.MinDelay = 5 * time.Second
		})
	if err != nil {
		panic(err)
	}

	return err
}

func parseInputGraph(filePath string) []Vertex {
	graph := make(map[uint64][]uint64)

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, "#") {
			continue
		}

		edge := strings.Split(line, "\t")
		src, _ := strconv.ParseUint(edge[0], 10, 32)
		dest, _ := strconv.ParseUint(edge[1], 10, 32)

		graph[uint64(src)] = append(graph[uint64(src)], uint64(dest))
		if graph[uint64(dest)] == nil {
			graph[uint64(dest)] = []uint64{}
		}
	}
	fmt.Printf("Successfully parsed %v nodes\n", len(graph))
	return graphToVertices(graph)
}

func graphToVertices(graph Graph) []Vertex {
	vertices := make([]Vertex, len(graph))

	idx := 0
	for vertexId, edges := range graph {
		hash := util.HashId(vertexId)
		vertices[idx] = Vertex{
			ID:    vertexId,
			Edges: edges,
			Hash:  hash,
		}
		idx++
	}

	return vertices
}
