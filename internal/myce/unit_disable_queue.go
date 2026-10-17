package myce

import "errors"

var ErrLegacyLocalDisableReceipts = errors.New("--local-disable-receipts is retired; reinstall the queue without this option using myce install queue")
