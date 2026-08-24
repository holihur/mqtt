.PHONY: build lint test bench run clean

BIN=bin/broker
GO=go

build:
	mkdir -p bin
	$(GO) build -o $(BIN) ./cmd/broker

lint:
	$(GO) vet ./...
	# golangci-lint if available
	which golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || true

test:
	$(GO) test -race -count=1 ./...

test-v:
	$(GO) test -race -count=1 -v ./...

run: build
	./$(BIN) -tcp :1883 -ws :8083 -redis 127.0.0.1:6379

run-memory: build
	./$(BIN) -tcp :1883 -ws :8083 -redis ""

bench:
	$(GO) test -run=^$$ -bench=. ./... -benchmem

clean:
	rm -rf bin
