.PHONY: test bench bench-matrix bench-db bench-queue bench-kafka build build-kafka test-kafka test-integration test-integration-kafka validate-fixtures

BIN := bin/market-replay
GO ?= go
CGO_ENABLED ?= 0

test:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test ./...

bench:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay bench --file testdata/btcusdt_depth.jsonl
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay bench --file testdata/sequence_gap.jsonl
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay bench --file testdata/solusdt_aggtrade.csv

bench-matrix:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay bench-matrix --runs 3 --bytes 65536 --output-dir tmp/bench-matrix

bench-db: build
	DATABASE_URL=$${DATABASE_URL:?DATABASE_URL is required} bin/market-replay db-bench --rows 50000 --lookups 1000 --insert-rows 10000

bench-queue: build
	REDIS_ADDR=$${REDIS_ADDR:?REDIS_ADDR is required} bin/market-replay queue-bench --jobs 1000

bench-kafka: build-kafka
	bin/kafka-replay bench --brokers $${KAFKA_BROKERS:-localhost:9092} --file $${KAFKA_BENCH_FILE:-tmp/btcusdt_10mb.jsonl}

build:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o $(BIN) ./cmd/market-replay
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/server ./cmd/server
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/worker ./cmd/worker

build-kafka:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -tags kafka -o bin/kafka-replay ./cmd/kafka-replay

test-kafka:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test -tags kafka ./internal/kafkastream ./cmd/kafka-replay

test-integration:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test -tags integration ./internal/store ./internal/queue

test-integration-kafka:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test -tags "integration kafka" ./internal/kafkastream ./cmd/kafka-replay

validate-fixtures:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay validate --file testdata/btcusdt_depth.jsonl
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay validate --file testdata/ethusdt_depth.jsonl
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay validate --file testdata/sequence_gap.jsonl
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay validate --file testdata/malformed.jsonl
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/market-replay validate --file testdata/solusdt_aggtrade.csv
