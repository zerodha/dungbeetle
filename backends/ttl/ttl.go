// Package ttl implements a simple way to register callback
// functions against TTLs, where the functions are triggered
// when the TTL's hit.
package ttl

import (
	"sync"
	"time"
)

// Item represents the TTL time and the function to execute.
type item struct {
	ttl time.Time
	f   func()
}

// TTL represents an instance of the TTL package.
type TTL struct {
	ttlMap   map[string]item
	mut      sync.Mutex
	interval time.Duration
}

// New returns a new TTL instance.
func New(interval time.Duration) *TTL {
	return &TTL{
		ttlMap:   make(map[string]item),
		interval: interval,
	}
}

// Add adds a new timer and a ballback.
func (t *TTL) Add(id string, d time.Duration, f func()) {
	t.mut.Lock()
	delete(t.ttlMap, id)
	t.ttlMap[id] = item{
		ttl: time.Now().Add(d),
		f:   f,
	}
	t.mut.Unlock()
}

// Run is a blocking function that runs forever,
// tracking TTL expiries and executing callbacks
// at set intervals.
func (t *TTL) Run() {
	for now := range time.Tick(t.interval) {
		t.mut.Lock()
		for id, i := range t.ttlMap {
			if i.ttl.Before(now) {
				i.f()
				delete(t.ttlMap, id)
			}
		}
		t.mut.Unlock()
	}
}
