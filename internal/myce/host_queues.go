package myce

import (
	"slices"
)

func (h HostControl) Queues() ([]string, error) {
	workers, err := h.workers()
	if err != nil {
		return nil, err
	}
	var queues []string
	for _, w := range workers {
		if w.Queue != "" && !slices.Contains(queues, w.Queue) {
			queues = append(queues, w.Queue)
		}
	}
	slices.Sort(queues)
	return queues, nil
}
