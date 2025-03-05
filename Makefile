all: test build

build: 
	go build -o bin/agnostic-etl-engine ./cmd

test:
	go test -v ./...

clean:
	rm -rf bin
