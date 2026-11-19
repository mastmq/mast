# syntax=docker/dockerfile:1

# The build stage runs on the builder's own platform and cross-compiles, so
# an arm64 image costs one more go build rather than a whole toolchain
# under QEMU emulation, which is several times slower.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Dependencies first so a source-only change reuses the cached layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off so the binary runs on a distroless base. Build info stays embedded
# so `mast --version` reports the commit without any ldflags.
ENV CGO_ENABLED=0
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/mast ./cmd/mast

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/mast /usr/local/bin/mast

# 1883 mqtt, 8883 mqtt over tls, 7422 leaf nodes, 6222 cluster routes,
# 8222 nats monitoring, 9090 metrics and pprof.
EXPOSE 1883 8883 7422 6222 8222 9090

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/mast"]
