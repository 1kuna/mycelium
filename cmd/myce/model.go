package main

import (
	"errors"

	"mycelium/internal/myce"
)

func modelCommand(args []string) error {
	if len(args) == 0 || args[0] == "--help" {
		return errors.New("catalog models load on demand through /v1 (docs/services.md)")
	}
	switch args[0] {
	case "fetch":
		return &myce.HTTPError{Code: "model_fetch_retired", Message: "download the model with its upstream client and stage it under the configured model root (docs/services.md)"}
	case "approve-local":
		return &myce.HTTPError{Code: "model_local_source_retired", Message: "stage verified weights under the configured model root (docs/services.md)"}
	case "pin", "unpin":
		return &myce.HTTPError{Code: "model_pin_retired", Message: "call the catalog model to load it on demand and set idle_timeout for retention (docs/services.md)"}
	case "sync":
		return &myce.HTTPError{Code: "model_sync_retired", Message: "stage verified weights under the configured model root and call the catalog model to load it on demand (docs/services.md)"}
	default:
		return errors.New("unknown model command")
	}
}
