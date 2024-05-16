.PHONY: clean format deps test coverage quality tidy all

# Run all targets
all: clean format quality coverage

# Remove generated files
clean:
	rm -rf coverage

# Format the Go code
format:
	go fmt ./...

tidy:
	go mod tidy

# Update dependencies
deps:
	go get -u ./...

# Check code quality
# curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.58.1
quality:
	go vet ./...
	golangci-lint run

test:
	go test -v ./...

# Generate a coverage report
coverage:
	mkdir -p coverage
	go test -v -coverprofile=coverage/coverage.out ./...
	go tool cover -html=coverage/coverage.out -o coverage/coverage.html
