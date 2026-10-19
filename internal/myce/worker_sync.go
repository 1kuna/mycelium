package myce

import (
	"context"
	"log"
	"path/filepath"
)

type workerCopy struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Real work wins; foreground loads wait for paused copies to release reservations.
func (rt *modelRuntime) syncCopies(plan, usage []CopyTask, pins []string, busy bool, keepActive ...bool) {
	rt.mu.Lock()
	rt.cachePins = map[string]bool{}
	for _, name := range pins {
		rt.cachePins[name] = true
	}
	rt.cacheUsage = map[string]int{}
	for _, u := range usage {
		rt.cacheUsage[u.Model] = u.Score
	}
	wanted := map[string]bool{}
	if !busy && !rt.closing {
		for _, task := range plan {
			wanted[task.Model] = true
		}
	}
	for name, c := range rt.copies {
		if !wanted[name] && !(len(keepActive) > 0 && keepActive[0]) {
			c.cancel()
		}
	}
	if busy || rt.closing {
		rt.mu.Unlock()
		return
	}
	for _, task := range plan {
		if len(rt.copies) >= rt.copyParallel {
			break
		}
		if rt.copies[task.Model] != nil {
			continue
		}
		var model Model
		for _, m := range rt.config.Models {
			if m.Name == task.Model {
				model = m
				break
			}
		}
		if model.Name == "" || !rt.localApprovalAvailable(model) || len(model.AllowedMachines) > 0 && !contains(model.AllowedMachines, rt.machine) {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		copy := &workerCopy{cancel: cancel, done: make(chan struct{})}
		rt.copies[model.Name] = copy
		rt.wg.Add(1)
		go func(score int) {
			defer rt.wg.Done()
			_, err := rt.cache.Precopy(ctx, model, score)
			if err != nil {
				rt.cache.noteCopyError(model, err)
			}
			rt.mu.Lock()
			delete(rt.copies, model.Name)
			close(copy.done)
			rt.mu.Unlock()
		}(task.Score)
	}
	rt.mu.Unlock()
}
func (rt *modelRuntime) cancelCopies() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, c := range rt.copies {
		c.cancel()
	}
}

func (rt *modelRuntime) pauseCopies() {
	rt.mu.Lock()
	done := []chan struct{}{}
	for _, c := range rt.copies {
		c.cancel()
		done = append(done, c.done)
	}
	rt.mu.Unlock()
	for _, ch := range done {
		<-ch
	}
}
func (rt *modelRuntime) copyTelemetry() ([]CopyReport, float64) {
	reports, speed := rt.cache.CopyReports()
	if speed > 0 && speed != rt.lastSavedSpeed {
		if e := saveWorkerJSON(rt.dir, "copy-speed.json", speed); e != nil {
			log.Printf("persist model copy speed: %v", e)
		} else {
			rt.lastSavedSpeed = speed
		}
	}
	return reports, speed
}
func (rt *modelRuntime) cacheProtected(key string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, m := range rt.models {
		if cacheKey(m.model) == key && m.state != "unloaded" && m.state != "failed" {
			return true
		}
	}
	for _, m := range rt.config.Models {
		if cacheKey(m) == key && rt.cachePins[m.Name] {
			return true
		}
	}
	return false
}
func (rt *modelRuntime) cachePopularity(key string) int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	key = filepath.Base(key)
	for _, m := range rt.config.Models {
		if cacheKey(m) == key {
			return rt.cacheUsage[m.Name]
		}
	}
	return 0
}
