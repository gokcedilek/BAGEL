package main

import (
	"context"
	"log"
	coord "project/bagel/proto/coord"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()

	conn, err := grpc.DialContext(
		ctx, "localhost:9990",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := coord.NewCoordClient(conn)

	query := coord.Query{
		ClientId:  "testclientId",
		QueryType: "test",
		Nodes:     []uint64{1, 2, 3},
		Graph:     "test",
	}

	res, err := client.StartQuery(ctx, &query)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("result: %v\n", res)

}
