package main

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"mycelium/internal/myce"
)

// adapterPiece is one keyed input of a plain entry. Args follow the base
// command and any CLI arguments; inputs are staged only for this piece.
type adapterPiece struct {
	Key    string            `yaml:"key"`
	Args   []string          `yaml:"args,omitempty"`
	Inputs map[string]string `yaml:"inputs,omitempty"`
}

// lowerAdapterPieces maps plain-entry pieces onto the queue's fan-out record.
// Per-piece inputs become ordinary profile inputs under the reserved piece
// prefix, so they are identified, frozen and uploaded with the entry's inputs.
func lowerAdapterPieces(p *executionProfile, pieces []adapterPiece) error {
	if len(pieces) == 0 {
		return nil
	}
	if p.Native != nil {
		return errors.New("pieces are for plain commands; native entries declare fan_out")
	}
	if p.FanOut != nil {
		return errors.New("choose pieces or fan_out")
	}
	if len(pieces) > 256 {
		return errors.New("pieces allows at most 256 entries")
	}
	inputs := maps.Clone(p.Inputs)
	if inputs == nil {
		inputs = map[string]string{}
	}
	for dest := range inputs {
		if strings.HasPrefix(dest, myce.PlainPiecePrefix) {
			return fmt.Errorf("input names under %s are reserved for pieces", myce.PlainPiecePrefix)
		}
	}
	f := &myce.FanOut{}
	seen := map[string]bool{}
	for _, piece := range pieces {
		if !adapterName.MatchString(piece.Key) || seen[piece.Key] {
			return fmt.Errorf("piece key %q must be a unique lowercase name", piece.Key)
		}
		seen[piece.Key] = true
		for dest, local := range piece.Inputs {
			if !profileInputName(dest) || strings.HasPrefix(dest, myce.PlainPiecePrefix) || local == "" {
				return fmt.Errorf("piece %s inputs require clean destination names and local paths", piece.Key)
			}
			inputs[myce.PlainPiecePrefix+piece.Key+"/"+dest] = local
		}
		f.Pieces = append(f.Pieces, myce.FanOutPiece{Key: piece.Key, Args: piece.Args})
	}
	p.Inputs = inputs
	p.FanOut = f
	return nil
}
