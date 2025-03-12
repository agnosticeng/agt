all: test build

build: 
	go build -o bin/agt ./cmd

test:
	go test -v ./...

clean:
	rm -rf bin
