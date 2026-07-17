.PHONY: test test/pubsub deps clean lint dev dev-clean

# Run tests
test:
	go test -v -race ./...

PUBSUB_TEST_HOST ?= localhost:5085
test/pubsub:
	JACK_COURIER_TEST_PUBSUB_HOST=$(PUBSUB_TEST_HOST) go test -v -race -count=1 -run Integration ./...

# Download dependencies
deps:
	go mod tidy
	go mod download

# Clean test cache
clean:
	go clean -testcache

# Lint
lint:
	golangci-lint run ./...

# Use local jack proto instead of AR (for local development)
dev:
	@VERSION=$$(grep 'jack/proto/jackpb' go.mod | head -1 | awk '{print $$2}') && \
	printf 'go 1.25.6\n\nuse .\n\nreplace github.com/clerk/jack/proto/jackpb %s => ../jack/proto/jackpb\n' "$$VERSION" > go.work
	@echo "Local dev mode enabled (go.work created). Using ../jack/proto/jackpb"

# Switch back to AR (remove local override)
dev-clean:
	@rm -f go.work go.work.sum
	@echo "Local dev mode disabled. Using AR version from go.mod."
