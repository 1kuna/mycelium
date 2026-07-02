package modelruntime

import (
	"strconv"

	"mycelium/internal/domain"
)

func EffectiveContextLength(preset domain.Preset) int {
	context := preset.ContextLength
	if preset.Backend != domain.BackendVLLM {
		return context
	}
	if maxModelLen := launchArgInt(preset.LaunchArgs, "--max-model-len"); maxModelLen > 0 && (context == 0 || maxModelLen < context) {
		return maxModelLen
	}
	return context
}

func launchArgInt(args []string, name string) int {
	for i := 0; i < len(args); i++ {
		if args[i] == name && i+1 < len(args) {
			value, err := strconv.Atoi(args[i+1])
			if err == nil && value > 0 {
				return value
			}
			return 0
		}
		if len(args[i]) > len(name)+1 && args[i][:len(name)+1] == name+"=" {
			value, err := strconv.Atoi(args[i][len(name)+1:])
			if err == nil && value > 0 {
				return value
			}
			return 0
		}
	}
	return 0
}
