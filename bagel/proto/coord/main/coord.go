package main

import (
	"context"
	"log"
	"net"
	coord "project/bagel/proto/coord"

	"google.golang.org/grpc"
)

// Server is the Logic handler for the server
// It has to fullfill the GRPC schema generated Interface
// In this case its only 1 function called Ping
type Server struct {
	coord.UnimplementedCoordServer
}

func (s *Server) StartQuery(ctx context.Context, q *coord.Query) (
	*coord.QueryResult,
	error,
) {
	return &coord.QueryResult{
		Query:  q,
		Result: nil,
		Error:  "no error",
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:9990")

	if err != nil {
		log.Fatalf("Error while listening : %v", err)
	}

	s := grpc.NewServer()
	coord.RegisterCoordServer(s, &Server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error while serving : %v", err)
	}

}
