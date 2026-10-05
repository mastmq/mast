default:
    @just --list

build:
    @echo '{{ BOLD + CYAN }}Building mast{{ NORMAL }}'
    go build -o mast ./cmd/mast

run *ARGS:
    go run ./cmd/mast {{ ARGS }}

test:
    go test -race -v ./... -covermode=atomic -coverprofile=coverage.out

# The hot paths, with allocations. BenchmarkConnect reports nats-subs/client,
# which must stay at zero: the control plane is one subscription per node.
bench PKG="./...":
    go test -run='^$' -bench=. -benchmem {{ PKG }}

# The topic codec is baked into stored subjects, so fuzz it before shipping.
fuzz TIME="60s":
    go test -run='^$' -fuzz=FuzzRoundTrip -fuzztime={{ TIME }} ./internal/domain/topic/

image:
    docker build -f build/package/Dockerfile -t mast:dev .

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
