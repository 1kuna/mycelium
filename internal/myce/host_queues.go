package myce

import (
	"errors"

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
func (h HostControl) ForgetQueue(queue string, force bool, configured func(HostWorker) (bool, error)) error {
	return errors.New("queue custody cannot be forgotten; reconcile the host instead")
}
