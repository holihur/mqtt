.PHONY: build lint test bench run clean dev dev-down dev-logs

BIN=bin/broker
GO=go

build:
	mkdir -p bin
	$(GO) build -o $(BIN) ./cmd/broker

lint:
	$(GO) vet ./...
	which golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || true

test:
	$(GO) test -race -count=1 ./...

test-v:
	$(GO) test -race -count=1 -v ./...

run: build
	./$(BIN) -tcp :1883 -ws :8083 -redis 127.0.0.1:6379 -pprof :6060

run-memory: build
	./$(BIN) -tcp :1883 -ws :8083 -redis "" -pprof :6060

# one-click dev: ensure redis, build, run broker with pprof/metrics
dev: build
	@echo "== dev: ensure redis =="
	@redis-cli ping >/dev/null 2>&1 || (echo "redis not running → docker compose up -d redis" && docker compose up -d redis && sleep 2 && redis-cli ping >/dev/null 2>&1 && echo "redis ready" || echo "redis still not ready, falling back to memory (broker will start without redis)")
	@echo "== dev: starting broker =="
	@echo "   mqtt://localhost:1883  (TCP)"
	@echo "   ws://localhost:8083/mqtt  (WebSocket)"
	@echo "   http://localhost:6060/debug/pprof  (pprof)"
	@echo "   http://localhost:6060/metrics  (prometheus)"
	@echo "   http://localhost:9090  (prometheus, if compose up)"
	@echo "   press Ctrl+C to stop"
	./$(BIN) -tcp :1883 -ws :8083 -redis 127.0.0.1:6379 -pprof :6060 -node dev

dev-down:
	@echo "== dev: stopping redis =="
	docker compose down 2>/dev/null || docker rm -f mqtt-redis 2>/dev/null || true

dev-logs:
	docker compose logs -f 2>/dev/null || docker logs -f mqtt-redis 2>/dev/null || true

bench:
	$(GO) test -run=^$$ -bench=. ./... -benchmem

clean:
	rm -rf bin
