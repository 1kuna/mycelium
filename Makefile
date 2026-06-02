.PHONY: fmt build vet test coverage smoke smoke-local smoke-fleet smoke-overlay smoke-mlx smoke-vllm smoke-b70 smoke-spark-vllm ci

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
	go run ./tools/covergate -profile all.out -min 0.85 -package-min 0.85 -package-prefix cmd/ -package-prefix internal/ -package-prefix pkg/ -package-prefix tools/ -require internal/scheduler=1.0 -require internal/lease=1.0 -require internal/peer=1.0 -require test/contract=1.0 -require test/fixtures=1.0 -require-file internal/node/admission.go=1.0 -require-file internal/peer/coordinator.go=1.0 -require-file internal/peer/recovery.go=1.0

smoke: smoke-local smoke-fleet

smoke-local:
	go test -count=1 -tags smoke ./test/smoke/... -run 'TestLocalLlamaCppConformance|TestLocalPhase1LoadServeTelemetryRequeueReaper|TestPhase2GatewayLocalLlamaCppSmoke|TestPhase3CatalogMaterializedPresetLoadsInNode|TestPhase4JoinedNodeGatewaySmoke' -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestLocalLlamaCppConformance -require TestLocalPhase1LoadServeTelemetryRequeueReaper -require TestPhase2GatewayLocalLlamaCppSmoke -require TestPhase3CatalogMaterializedPresetLoadsInNode -require TestPhase4JoinedNodeGatewaySmoke

smoke-fleet:
	go test -count=1 -tags smoke ./test/smoke/... -run 'TestPhase4JoinedNodeGatewaySmoke|TestPhase6FederationSubmitAnywhereSmoke' -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestPhase4JoinedNodeGatewaySmoke -require TestPhase6FederationSubmitAnywhereSmoke

smoke-overlay:
	go test -count=1 -tags smoke ./test/smoke/... -run TestLibp2pOverlaySmoke -timeout 5m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestLibp2pOverlaySmoke

smoke-mlx:
	go test -count=1 -tags smoke ./test/smoke/... -run TestLocalMLXConformance -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestLocalMLXConformance

smoke-vllm:
	go test -count=1 -tags smoke ./test/smoke/... -run TestLocalVLLMConformance -timeout 20m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestLocalVLLMConformance

smoke-b70:
	MYCELIUM_EXPECT_INTEL_ARC_B70=1 go test -count=1 -tags smoke ./test/smoke/... -run TestLinuxIntelArcB70HardwareDiscovery -timeout 5m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestLinuxIntelArcB70HardwareDiscovery

smoke-spark-vllm:
	go test -count=1 -tags smoke ./test/smoke/... -run TestSparkVLLMPeerRoutingSmoke -timeout 30m -json > $(SMOKE_JSON)
	go run ./tools/smokegate -json $(SMOKE_JSON) -require TestSparkVLLMPeerRoutingSmoke

ci: fmt build vet test coverage
