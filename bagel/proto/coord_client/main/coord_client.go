package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	coord "project/bagel/proto/coord"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// loadTLSCfg will load a certificate and create a tls config
func loadTLSCfg() *tls.Config {
	b, _ := ioutil.ReadFile("../../../cert/server.crt")
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		log.Fatal("credentials: failed to append certificates")
	}
	config := &tls.Config{
		InsecureSkipVerify: false,
		RootCAs:            cp,
	}
	return config
}

func main() {
	ctx := context.Background()

	creds := credentials.NewTLS(loadTLSCfg())

	conn, err := grpc.DialContext(
		ctx, "localhost:9990",
		grpc.WithTransportCredentials(creds),
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

	log.Printf("client received result: %v\n", res)

}
