package myce

import (
	"encoding/json"
	"errors"
	"strconv"

	"gopkg.in/yaml.v3"
)

func (g GPUSelection) MarshalJSON() ([]byte, error) {
	out := map[string]any{}
	if g.Count != nil {
		out["count"] = *g.Count
	}
	if g.Devices != nil {
		out["devices"] = g.Devices
	}
	return json.Marshal(out)
}

func (g *GPUSelection) UnmarshalJSON(data []byte) error {
	var n yaml.Node
	if err := yaml.Unmarshal(data, &n); err != nil {
		return err
	}
	if len(n.Content) != 1 {
		return reject("gpu_selector_invalid", nil)
	}
	return g.UnmarshalYAML(n.Content[0])
}

func (g *GPUSelection) UnmarshalYAML(n *yaml.Node) error {
	invalid := func() error {
		return reject("gpu_selector_invalid", errors.New("GPU selection requires a positive integer count and a nonempty set of canonical device IDs"))
	}
	if n.Kind != yaml.MappingNode {
		return invalid()
	}
	var value GPUSelection
	seen := map[string]bool{}
	for i := 0; i < len(n.Content); i += 2 {
		key, item := n.Content[i].Value, n.Content[i+1]
		if seen[key] {
			return invalid()
		}
		seen[key] = true
		switch key {
		case "count":
			if item.Tag != "!!int" {
				return invalid()
			}
			v, err := strconv.Atoi(item.Value)
			if err != nil {
				return invalid()
			}
			value.Count = &v
		case "devices":
			if item.Kind != yaml.SequenceNode {
				return invalid()
			}
			value.Devices = []string{}
			for _, id := range item.Content {
				if id.Tag != "!!str" {
					return invalid()
				}
				value.Devices = append(value.Devices, id.Value)
			}
		default:
			return invalid()
		}
	}
	*g = value
	return nil
}
