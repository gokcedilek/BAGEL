package mongodb

import (
	"context"
	"log"
	"math"
	"os"
	"project/database"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetDatabaseClient() *mongo.Client {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("error loading env file: %v\n", err)
	}
	password := os.Getenv("DB_PASSWORD")

	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().
		ApplyURI(
			"mongodb+srv://bagel:" +
				password + "@bagel.gd7kkby.mongodb." +
				"net/?retryWrites=true&w=majority",
		).
		SetServerAPIOptions(serverAPIOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("connected to mongodb client: %v\n", client)
	return client
}

func createBatches(vertices []database.Vertex) [][]interface{} {
	numBatches := int(
		math.Ceil(
			float64(len(vertices)) / float64(
				database.MAXIMUM_ITEMS_PER_BATCH,
			),
		),
	)
	lastBatchSize := len(vertices) % database.MAXIMUM_ITEMS_PER_BATCH

	batches := make([][]interface{}, numBatches)
	for b := 0; b < numBatches; b++ {
		if b == numBatches-1 {
			batches[b] = make([]interface{}, lastBatchSize)
			continue
		}
		batches[b] = make([]interface{}, database.MAXIMUM_ITEMS_PER_BATCH)
	}

	for idx, vertex := range vertices {
		batchIdx := idx / database.MAXIMUM_ITEMS_PER_BATCH
		vertexIdx := idx % database.MAXIMUM_ITEMS_PER_BATCH
		batches[batchIdx][vertexIdx] = bson.D{
			{"ID", strconv.FormatUint(vertex.ID, 10)}, {
				"Edges",
				formatEdges(vertex.Edges),
			},
			{
				"Hash", strconv.FormatUint(
					vertex.Hash, 10,
				),
			},
		}
	}

	return batches
}

func formatEdges(edges []uint64) []string {
	formattedEdges := make([]string, len(edges))
	for idx, edge := range edges {
		formattedEdges[idx] = strconv.FormatUint(edge, 10)
	}
	return formattedEdges
}

func BatchInsertVertices(
	collection *mongo.Collection,
	batches [][]interface{},
) {
	numBatches := len(batches)
	for b := 0; b < numBatches; b++ {
		_, err := collection.InsertMany(context.TODO(), batches[b])
		if err != nil {
			log.Fatalf("Failed to upload batch %v. Here's why %v\n", b, err)
		}
		log.Printf("Successfully uploaded batch %v/%v\n", b+1, numBatches)
	}
	log.Printf("%v batches added to %v", numBatches, collection.Name())
}

func GetCollection(client *mongo.Client, tableName string) *mongo.Collection {
	db := client.Database("bagel")
	collection := db.Collection(tableName)
	return collection
}

func AddGraph(client *mongo.Client, filePath string, tableName string) {
	vertices := database.ParseInputGraph(filePath)
	log.Printf("vertices: %v\n", vertices)

	batches := createBatches(vertices)
	log.Printf("batches: %v\n", batches)

	collection := GetCollection(client, tableName)
	BatchInsertVertices(collection, batches)
}
