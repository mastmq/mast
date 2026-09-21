default:
    @just --list

build:
    @echo '{{ BOLD + CYAN }}Building mast{{ NORMAL }}'
    go build -o mast ./cmd/mast

run *ARGS:
    go run ./cmd/mast {{ ARGS }}

test:
    go test -race -v ./... -covermode=atomic -coverprofile=coverage.out

# The topic codec is baked into stored subjects, so fuzz it before shipping.
fuzz TIME="60s":
    go test -run='^$' -fuzz=FuzzRoundTrip -fuzztime={{ TIME }} ./internal/domain/topic/

lint:
    golangci-lint run -c .golangci.yml

fix:
    go fix ./...
    golangci-lint run -c .golangci.yml --fix

tidy:
    go mod tidy

update:
    go get -u ./...
    go mod tidy
