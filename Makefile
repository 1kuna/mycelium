.PHONY: fmt build vet test coverage smoke ci

fmt:
	test -z "$$(gofmt -l .)"

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./... -race

coverage:
	go test ./... -covermode=atomic -coverprofile=all.out
	go run ./tools/covergate -profile all.out -min 0.85 -require internal/scheduler=1.0 -require internal/lease=1.0 -require test/fixtures=1.0

smoke:
	go test -tags smoke ./test/smoke/... -timeout 20m

ci: fmt build vet test coverage
