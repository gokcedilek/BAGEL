.PHONY: all clean test test-worker

all: worker coord client db conf

worker:
	go build -o bin/worker ./cmd/worker

coord:
	go build -o bin/coord ./cmd/coord

client:
	go build -o bin/client ./cmd/client

db:
	go build -o bin/database ./cmd/database

conf:
	go build -o bin/config ./cmd/config

clean:
	rm -f bin/*
	go clean -testcache

test:
	go test ./bagel -test.v