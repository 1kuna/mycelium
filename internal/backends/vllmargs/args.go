package vllmargs

import (
	"fmt"
	"strconv"
	"strings"
)

const GPUMemoryUtilizationFlag = "--gpu-memory-utilization"

func Normalize(args []string) ([]string, error) {
	out := make([]string, 0, len(args))
	var util string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == GPUMemoryUtilizationFlag {
			if i+1 >= len(args) {
				return nil, fmt.Errorf("%s missing value", GPUMemoryUtilizationFlag)
			}
			util = args[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, GPUMemoryUtilizationFlag+"=") {
			util = strings.TrimPrefix(arg, GPUMemoryUtilizationFlag+"=")
			continue
		}
		out = append(out, arg)
	}
	if util == "" {
		return out, nil
	}
	if _, err := ParseUtilization(util); err != nil {
		return nil, err
	}
	return append(out, GPUMemoryUtilizationFlag, util), nil
}

func GPUMemoryUtilization(args []string) (float64, bool, error) {
	normalized, err := Normalize(args)
	if err != nil {
		return 0, false, err
	}
	for i, arg := range normalized {
		if arg == GPUMemoryUtilizationFlag {
			value, err := ParseUtilization(normalized[i+1])
			return value, true, err
		}
	}
	return 0, false, nil
}

func ParseUtilization(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, err
	}
	if value <= 0 || value > 1 {
		return 0, fmt.Errorf("value must be > 0 and <= 1")
	}
	return value, nil
}
