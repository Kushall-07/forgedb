# Phase 14 convenience targets. These are thin wrappers around commands
# documented in full in deploy/README.md -- nothing here does anything
# deploy/README.md doesn't already explain by hand.

.PHONY: build test vet fmt docker-build docker-up docker-down docker-logs docker-status docker-reset smoke-test

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

docker-status:
	docker compose ps

# docker-reset DELETES all three nodes' persistent volumes -- see
# deploy/README.md section 15 before running this.
docker-reset:
	docker compose down -v

smoke-test:
	bash deploy/scripts/smoke-test.sh
