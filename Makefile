.PHONY: test deps clean lint dev dev-clean

# Run tests
test:
	go test -v -race ./...

# Download dependencies
deps:
	go mod tidy
	go mod download

# Clean test cache
clean:
	go clean -testcache

# Lint
lint:
	go vet ./...

# Use local jack-service proto instead of AR (for local development)
dev:
	@VERSION=$$(grep 'jack-service/proto/jackpb' go.mod | head -1 | awk '{print $$2}') && \
	printf 'go 1.25.6\n\nuse .\n\nreplace github.com/clerk/jack-service/proto/jackpb %s => ../jack-service/proto/jackpb\n' "$$VERSION" > go.work
	@echo "Local dev mode enabled (go.work created). Using ../jack-service/proto/jackpb"

# Switch back to AR (remove local override)
dev-clean:
	@rm -f go.work go.work.sum
	@echo "Local dev mode disabled. Using AR version from go.mod."
