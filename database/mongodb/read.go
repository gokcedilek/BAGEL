package mongodb

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DBVertex struct {
	ID    string
	Edges []string
	Hash  string
}

type Vertex struct {
	ID    uint64
	Edges []uint64
	Hash  uint64
}

func GetVertexById(
	collection *mongo.Collection, vertexId uint64,
) (Vertex, error) {
	var dbVertex DBVertex
	if err := collection.FindOne(
		context.Background(), bson.M{"ID": strconv.FormatUint(vertexId, 10)},
	).Decode(&dbVertex); err != nil {
		log.Printf("error decoding vertex: %v\n", err)
	}

	vertex := parseDBVertex(dbVertex)

	return vertex, nil
}

func parseDBVertex(dbVertex DBVertex) Vertex {
	id, _ := strconv.ParseUint(dbVertex.ID, 10, 64)
	edges := make([]uint64, len(dbVertex.Edges))

	for idx, edge := range dbVertex.Edges {
		edges[idx], _ = strconv.ParseUint(edge, 10, 64)
	}

	hash, _ := strconv.ParseUint(dbVertex.Hash, 10, 64)

	return Vertex{
		ID:    id,
		Edges: edges,
		Hash:  hash,
	}
}

func GetPartitionForWorkerX(
	collection *mongo.Collection, numPartitions int,
	workerId int,
) ([]Vertex, error) {
	if !isPartitionCached(collection, numPartitions) {
		log.Printf("partition not cached: %v\n", numPartitions)
		if err := PartitionGraph(collection, numPartitions); err != nil {
			return nil, err
		}
	} else {
		log.Printf("partition cached: %v\n", numPartitions)
	}

	// read the vertices in partition
	partitionName := getPartitionName(numPartitions)
	cursor, err := collection.Find(
		context.TODO(), bson.M{
			partitionName: strconv.FormatInt(
				int64(
					workerId,
				), 10,
			),
		},
	)

	var dbVertices []DBVertex
	if err = cursor.All(context.TODO(), &dbVertices); err != nil {
		log.Printf("error reading vertices: %v\n", err)
		return nil, err
	}

	var vertices []Vertex
	for _, dbVertex := range dbVertices {
		vertex := parseDBVertex(dbVertex)
		vertices = append(vertices, vertex)
	}

	log.Printf("found partition %v vertices %v\n", workerId, vertices)
	return vertices, nil
}

func PartitionGraph(collection *mongo.Collection, numPartitions int) error {
	cursor, err := collection.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Printf("error fetching vertices: %v\n", err)
		return err
	}

	var vertices []DBVertex
	if err = cursor.All(context.TODO(), &vertices); err != nil {
		log.Printf("error reading vertices: %v\n", err)
		return err
	}

	for _, vertex := range vertices {
		fmt.Printf("read vertex: %v\n", vertex)
		hash, _ := strconv.ParseUint(vertex.Hash, 0, 64)
		partition := hash % uint64(numPartitions)
		partitionName := getPartitionName(numPartitions)

		// create update body
		updateBodyMap := make(map[string]string)
		updateBodyMap[partitionName] = strconv.FormatUint(
			partition, 10,
		)

		// add partitionName field to each vertex
		if _, err = collection.UpdateOne(
			context.TODO(), bson.M{"ID": vertex.ID}, bson.D{
				{"$set", updateBodyMap},
			},
		); err != nil {
			log.Printf(
				"failed to update vertex %v to partition %v\n",
				vertex, partition,
			)
			return err
		}
		fmt.Printf(
			"updated vertex id %v with hash %v to partition map %v\n",
			vertex.ID, vertex.Hash, updateBodyMap,
		)
	}
	return nil
}

func getPartitionName(numPartitions int) string {
	return fmt.Sprintf("P%d", numPartitions)
}

func isPartitionCached(collection *mongo.Collection, numPartitions int) bool {
	partitionName := getPartitionName(numPartitions)

	cursor, err := collection.Find(
		context.TODO(), bson.D{
			{
				partitionName, bson.D{
					{"$exists", true},
				},
			},
		}, options.Find().SetLimit(1),
	)
	if err != nil {
		log.Fatalf(
			"Failed to verify partition cache for %v, error %v\n",
			numPartitions, err,
		)
	}

	var verticesInPartition []bson.M
	if err = cursor.All(context.TODO(), &verticesInPartition); err != nil {
		log.Fatal(err)
	}
	log.Printf("cached partition vertex: %v\n", verticesInPartition)

	return len(verticesInPartition) != 0
}
