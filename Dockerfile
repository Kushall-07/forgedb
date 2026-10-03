# Phase 14 deployment image for ForgeDB. Multi-stage build: a Go builder
# stage compiles a static binary, and a small Alpine runtime stage runs
# it as a non-root user with no build tooling present. This is a
# development/demo deployment image (see
# docs/deployment/phase14-docker-deployment.md's security notes) -- it
# intentionally does not include TLS material or production secrets
# handling, neither of which this project implements.

# ---- builder ----------------------------------------------------------
FROM golang:1.27.1-alpine AS builder

WORKDIR /src

# Dependencies are downloaded in their own layer so an unrelated source
# change does not force a re-download.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a statically linked binary with no libc
# dependency, which is what lets the runtime stage below be as small and
# dependency-free as it is. cmd/forgedb (the server), cmd/forge-client
# (the CLI client), and cmd/forge-gateway (the leader-aware HTTP
# reverse proxy -- see internal/gateway's package doc comment) are all
# built, since the same image is convenient for ad hoc client use (e.g.
# `docker run --rm --network forgedb-net forgedb:latest forge-client
# ...`) and for running the gateway as its own container (see
# docker-compose.yml's forge-gateway service) without pulling a second
# image.
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/forgedb ./cmd/forgedb && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/forge-client ./cmd/forge-client && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/forge-gateway ./cmd/forge-gateway

# ---- runtime ------------------------------------------------------------
FROM alpine:3.20

# ca-certificates: harmless now (Phase 14 uses plaintext gRPC -- see the
# deployment doc's security notes) and needed the moment any future TLS
# work lands. curl: used only by HEALTHCHECK below, against this
# process's own /health endpoint -- never installed for any other
# purpose, and never used to fetch anything from the network.
RUN apk add --no-cache ca-certificates curl && \
    addgroup -g 10001 forgedb && \
    adduser -D -u 10001 -G forgedb -h /home/forgedb forgedb && \
    mkdir -p /data && chown forgedb:forgedb /data

COPY --from=builder /out/forgedb /usr/local/bin/forgedb
COPY --from=builder /out/forge-client /usr/local/bin/forge-client
COPY --from=builder /out/forge-gateway /usr/local/bin/forge-gateway

USER forgedb
WORKDIR /home/forgedb
VOLUME ["/data"]

# These are the in-container defaults every Compose service in
# docker-compose.yml overrides with its own NODE_ID, PEERS, etc. -- see
# config.Load's documented environment variables
# (internal/config/load.go). Every node listens on the same internal
# ports; docker-compose.yml maps each to a distinct host port.
ENV HTTP_ADDR=0.0.0.0:8080 \
    GRPC_ADDR=0.0.0.0:9090 \
    DATA_DIR=/data \
    LOG_LEVEL=info

EXPOSE 8080 9090

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=5 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["forgedb"]
