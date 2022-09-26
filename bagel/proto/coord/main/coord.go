package main

import (
	"context"
	"log"
	"net"
	"net/http"
	coord "project/bagel/proto/coord"
	"time"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type grpcMultiplexer struct {
	*grpcweb.WrappedGrpcServer
}

func GenerateTLSApi(pemPath, keyPath string) (*grpc.Server, error) {
	cred, err := credentials.NewServerTLSFromFile(pemPath, keyPath)
	if err != nil {
		return nil, err
	}

	s := grpc.NewServer(
		grpc.Creds(cred),
	)
	return s, nil
}

// Handler is used to route requests to either grpc or to regular http
func (m *grpcMultiplexer) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if m.IsGrpcWebRequest(r) {
				m.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		},
	)
}

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
		Error:  "no error!",
	}, nil
}

func main() {

	apiServer, err := GenerateTLSApi(
		"../../../cert/server.crt",
		"../../../cert/server.key",
	)
	if err != nil {
		log.Fatalf("Error while generating TLS API: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:9090")

	if err != nil {
		log.Fatalf("Error while listening : %v", err)
	}

	//s := grpc.NewServer()
	//coord.RegisterCoordServer(s, &Server{})
	coord.RegisterCoordServer(apiServer, &Server{})

	go func() {
		if err := apiServer.Serve(lis); err != nil {
			log.Fatalf("Error while serving : %v", err)
		}
	}()
	//if err := apiServer.Serve(lis); err != nil {
	//	log.Fatalf("Error while serving : %v", err)
	//}

	grpcWebServer := grpcweb.WrapServer(apiServer)

	multiplex := grpcMultiplexer{grpcWebServer}

	router := http.NewServeMux()

	webapp := http.FileServer(http.Dir("../../../../client/build"))

	router.Handle("/", multiplex.Handler(webapp))

	srv := &http.Server{
		Addr:         "localhost:8080",
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	log.Fatal(
		srv.ListenAndServeTLS(
			"../../../cert/server.crt",
			"../../../cert/server.key",
		),
	)

}
