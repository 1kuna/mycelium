.PHONY: fmt build vet test coverage smoke smoke-local smoke-fleet smoke-mlx smoke-vllm ci

SMOKE_JSON ?= smoke.out

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
	go run ./tools/covergate -profile all.out -min 0.85 -package-min 0.85 -package-prefix internal/ -package-prefix pkg/ -require internal/scheduler=1.0 -require internal/lease=1.0 -require internal/peer=1.0 -require test/contract=1.0 -require test/fixtures=1.0

smoke:
	go test -count=1 -tags smoke ./test/smoke/... -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -min-pass 1

smoke-local:
	go test -count=1 -tags smoke ./test/smoke/... -run 'TestLocalLlamaCppConformance|TestLocalPhase1LoadServeTelemetryRequeueReaper|TestPhase2GatewayLocalLlamaCppSmoke|TestPhase3CatalogMaterializedPresetLoadsInNode|TestPhase4JoinedNodeGatewaySmoke' -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestLocalLlamaCppConformance -require TestLocalPhase1LoadServeTelemetryRequeueReaper -require TestPhase2GatewayLocalLlamaCppSmoke -require TestPhase3CatalogMaterializedPresetLoadsInNode -require TestPhase4JoinedNodeGatewaySmoke

smoke-fleet:
	go test -count=1 -tags smoke ./test/smoke/... -run 'TestPhase4JoinedNodeGatewaySmoke|TestPhase6FederationSubmitAnywhereSmoke' -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestPhase4JoinedNodeGatewaySmoke -require TestPhase6FederationSubmitAnywhereSmoke

smoke-mlx:
	go test -count=1 -tags smoke ./test/smoke/... -run TestLocalMLXConformance -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestLocalMLXConformance

smoke-vllm:
	go test -count=1 -tags smoke ./test/smoke/... -run TestLocalVLLMConformance -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestLocalVLLMConformance

ci: fmt build vet test coverage
