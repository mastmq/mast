# The drill and mastbench, run inside the compose network by fire.sh.
FROM golang:1.27 AS build
ARG BENCH_VERSION=latest
WORKDIR /src
COPY drill/ ./
RUN CGO_ENABLED=0 go build -o /x/drill . \
 && CGO_ENABLED=0 GOBIN=/x go install github.com/mastmq/bench/cmd/mastbench@${BENCH_VERSION}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /x/ /x/
