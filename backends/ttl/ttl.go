// Package ttl implements a simple way to register callback
// functions against TTLs, where the functions are triggered
// when the TTL's hit.
package ttl

import (
	"sync"
	"time"
)

// TTL represents an instance of the TTL package.
type TTL struct {
	ttlMap   map[time.Time]func()
	mut      sync.Mutex
	interval time.Duration
}

// New returns a new TTL instance.
func New(interval time.Duration) *TTL {
	return &TTL{
		ttlMap:   make(map[time.Time]func()),
		interval: interval,
	}
}

// Add adds a new timer and a ballback.
func (t *TTL) Add(d time.Duration, f func()) {
	t.mut.Lock()
	t.ttlMap[time.Now().Add(d)] = f
	t.mut.Unlock()
}

// Run is a blocking function that runs forever,
// tracking TTL expiries and executing callbacks
// at set intervals.
func (t *TTL) Run() {
	for now := range time.Tick(t.interval) {
		t.mut.Lock()
		for d, f := range t.ttlMap {
			if d.Before(now) {
				f()
				delete(t.ttlMap, d)
			}
		}
		t.mut.Unlock()
	}
}
