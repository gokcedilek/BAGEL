package database

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const CENTRAL_DB_NAME = "bagel-db"
const DEFAULT_REGION = "us-east-2"
const MAXIMUM_ITEMS_PER_BATCH = 5

type Graph map[uint64][]uint64

type Vertex struct {
	ID    uint64
	Edges []uint64
	Hash  uint64
}

var DB *dynamodb.Client

func GetDynamoClient() *dynamodb.Client {

	if DB != nil {
		return DB
	}

	// commented for later; needs to specify if we are connecting to live endpoint..
	//config, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(DEFAULT_REGION))
	config, err := getLocalDynamoConfiguration()

	if err != nil {
		log.Fatalf("unable to load SDK config %v", err)
	}

	DB = dynamodb.NewFromConfig(config)
	return DB
}

func getLocalDynamoConfiguration() (aws.Config, error) {
	config, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(DEFAULT_REGION),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(
					service, region string, options ...interface{},
				) (aws.Endpoint, error) {
					return aws.Endpoint{URL: "http://localhost:8000"}, nil
				},
			),
		),
		config.WithCredentialsProvider(
			credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     "local",
					SecretAccessKey: "local",
					SessionToken:    "local",
					Source:          "Hard-coded credentials, irrelvant values for local DynamoDB",
				},
			},
		),
	)

	return config, err
}

func GetVertexByID(
	svc *dynamodb.Client, vertexId int64, tableName string,
) (Vertex, error) {
	if svc == nil {
		svc = GetDynamoClient()
	}

	res, err := svc.GetItem(
		context.TODO(), &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"ID": &types.AttributeValueMemberN{
					Value: strconv.FormatInt(
						vertexId, 10,
					),
				},
			},
		},
	)

	if err != nil {
		return Vertex{}, err
	}

	vertex := Vertex{}
	attributevalue.UnmarshalMap(res.Item, &vertex)
	return vertex, nil
}

func GetPartitionForWorkerX(
	svc *dynamodb.Client, tableName string, partitionNum int, worker int,
) ([]Vertex, error) {
	if !IsPartitionCached(svc, tableName, partitionNum) {
		PartitionGraph(svc, tableName, partitionNum)
	}

	vertices := make([]Vertex, 0)
	p := dynamodb.NewScanPaginator(
		svc, &dynamodb.ScanInput{
			TableName:        aws.String(tableName),
			FilterExpression: aws.String("#partition = :partitionNum"),
			ExpressionAttributeNames: map[string]string{
				"#partition": getNumberOfWorkersXName(partitionNum),
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":partitionNum": &types.AttributeValueMemberN{
					Value: strconv.FormatInt(
						int64(worker), 10,
					),
				},
			},
		},
	)

	for p.HasMorePages() {
		paged, err := p.NextPage(context.TODO())

		if err != nil {
			return nil, fmt.Errorf("failed to get partition %w", err)
		}

		var paginatedVertices []Vertex
		err = attributevalue.UnmarshalListOfMaps(
			paged.Items, &paginatedVertices,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal %w", err)
		}
		vertices = append(vertices, paginatedVertices...)
	}

	return vertices, nil
}

func PartitionGraph(
	svc *dynamodb.Client, tableName string, numPartition int,
) error {
	if IsPartitionCached(svc, tableName, numPartition) {
		fmt.Printf(
			"cache for %d worker partition exists in %s\n", numPartition,
			tableName,
		)
		return nil
	}

	vertices, err := GetAllVertices(svc, tableName)
	if err != nil {
		return err
	}

	batches := CreatePartitionBatches(vertices, numPartition)
	BatchInsertVertices(svc, tableName, batches)

	return nil
}

func GetAllVertices(svc *dynamodb.Client, tableName string) ([]Vertex, error) {
	p := dynamodb.NewScanPaginator(
		svc, &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		},
	)

	var vertices []Vertex

	for p.HasMorePages() {
		page, err := p.NextPage(context.TODO())

		if err != nil {
			return nil, fmt.Errorf("failed to scan Items %w", err)
		}

		var pagedVertices []Vertex
		err = attributevalue.UnmarshalListOfMaps(page.Items, &pagedVertices)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal Items %w", err)
		}

		vertices = append(vertices, pagedVertices...)
	}

	return vertices, nil
}

func IsPartitionCached(
	svc *dynamodb.Client, tableName string, numPartition int,
) bool {
	limit := int32(1)
	partitionName := getNumberOfWorkersXName(numPartition)

	fmt.Println("tableName: ", tableName)

	out, err := svc.Scan(
		context.TODO(), &dynamodb.ScanInput{
			TableName: aws.String(tableName),
			FilterExpression: aws.String(
				fmt.Sprintf(
					"attribute_exists(%s)", partitionName,
				),
			),
			Limit: &limit,
		},
	)

	if err != nil {
		log.Fatalf("Failed to verify partition cache %v\n", err)
	}

	return len(out.Items) != 0
}

func BatchInsertVertices(
	svc *dynamodb.Client, tableName string, batches [][]types.WriteRequest,
) {
	numBatches := len(batches)
	for b := 0; b < numBatches; b++ {
		_, err := svc.BatchWriteItem(
			context.TODO(), &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					tableName: batches[b],
				},
			},
		)

		if err != nil {
			log.Fatalf("Failed to upload batch %v. Here's why %v\n", b, err)
		}

		log.Printf("Successfully uploaded batch %v/%v\n", b+1, numBatches)
	}

	log.Printf("%v batches added to %v", numBatches, tableName)
}

func CreateBatches(vertices []Vertex) [][]types.WriteRequest {
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

	return batches
}

func CreatePartitionBatches(
	vertices []Vertex, numPartitions int,
) [][]types.WriteRequest {
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
		batches[b][v] = marshalVertexForPartition(vertex, numPartitions)
	}

	return batches
}

func marshalVertexWriteReq(vertex Vertex) types.WriteRequest {
	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: map[string]types.AttributeValue{
				"ID": &types.AttributeValueMemberN{
					Value: strconv.FormatUint(
						vertex.ID, 10,
					),
				},
				"Edges": &types.AttributeValueMemberL{Value: edgesToAttributeValueSlice(vertex.Edges)},
				"Hash": &types.AttributeValueMemberN{
					Value: strconv.FormatUint(
						vertex.Hash, 10,
					),
				},
			},
		},
	}
}

func marshalVertexForPartition(
	vertex Vertex, numPartitions int,
) types.WriteRequest {
	partition := vertex.Hash % uint64(numPartitions)
	partitionAttrName := getNumberOfWorkersXName(numPartitions)
	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: map[string]types.AttributeValue{
				"ID": &types.AttributeValueMemberN{
					Value: strconv.FormatUint(
						vertex.ID, 10,
					),
				},
				"Edges": &types.AttributeValueMemberL{Value: edgesToAttributeValueSlice(vertex.Edges)},
				"Hash": &types.AttributeValueMemberN{
					Value: strconv.FormatUint(
						vertex.Hash, 10,
					),
				},
				partitionAttrName: &types.AttributeValueMemberN{
					Value: strconv.FormatUint(partition, 10),
				},
			},
		},
	}
}

func edgesToAttributeValueSlice(edges []uint64) []types.AttributeValue {
	as := make([]types.AttributeValue, len(edges))
	for idx, edge := range edges {
		as[idx] = &types.AttributeValueMemberN{
			Value: strconv.FormatUint(
				edge, 10,
			),
		}
	}
	return as
}

func getNumberOfWorkersXName(x int) string {
	return fmt.Sprintf("P%d", x)
}
