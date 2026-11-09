// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const cacheFormatVersion = 1

type cacheEntry[V any] struct {
	Val V         `json:"v"`
	Exp time.Time `json:"exp"`
}

type cacheFile[V any] struct {
	Version int                      `json:"version"`
	SavedAt time.Time                `json:"saved_at"`
	Entries map[string]cacheEntry[V] `json:"entries"`
}

// TTLCache is a concurrency-safe key/value cache whose entries expire.
// If path is non-empty it can be loaded from and saved to a JSON file, so
// results survive restarts and external lookups are not repeated.
type TTLCache[V any] struct {
	name    string
	path    string
	maxSize int

	mu    sync.RWMutex
	m     map[string]cacheEntry[V]
	dirty bool

	hits   atomic.Uint64
	misses atomic.Uint64
}

func NewTTLCache[V any](name, path string, maxSize int) *TTLCache[V] {
	return &TTLCache[V]{name: name, path: path, maxSize: maxSize, m: make(map[string]cacheEntry[V])}
}

// Get returns the value for k if present and not expired.
func (c *TTLCache[V]) Get(k string) (V, bool) {
	c.mu.RLock()
	e, ok := c.m[k]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.Exp) {
		c.misses.Add(1)
		var zero V
		return zero, false
	}
	c.hits.Add(1)
	return e.Val, true
}

// Peek is Get without touching the hit/miss counters.
func (c *TTLCache[V]) Peek(k string) (V, bool) {
	c.mu.RLock()
	e, ok := c.m[k]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.Exp) {
		var zero V
		return zero, false
	}
	return e.Val, true
}

func (c *TTLCache[V]) Set(k string, v V, ttl time.Duration) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxSize > 0 && len(c.m) >= c.maxSize {
		c.evictLocked(now)
	}
	c.m[k] = cacheEntry[V]{Val: v, Exp: now.Add(ttl)}
	c.dirty = true
}

// evictLocked drops expired entries; if still full, drops the entries closest
// to expiry until the cache is at 90% of maxSize.
func (c *TTLCache[V]) evictLocked(now time.Time) {
	for k, e := range c.m {
		if now.After(e.Exp) {
			delete(c.m, k)
		}
	}
	target := c.maxSize * 9 / 10
	for len(c.m) > target {
		var oldestK string
		var oldest time.Time
		for k, e := range c.m {
			if oldestK == "" || e.Exp.Before(oldest) {
				oldestK, oldest = k, e.Exp
			}
		}
		delete(c.m, oldestK)
	}
}

func (c *TTLCache[V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

func (c *TTLCache[V]) Stats() (hits, misses uint64) { return c.hits.Load(), c.misses.Load() }

// Load reads the cache file, skipping expired entries. A missing file is not an error.
func (c *TTLCache[V]) Load() (int, error) {
	if c.path == "" {
		return 0, nil
	}
	b, err := os.ReadFile(c.path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var f cacheFile[V]
	if err := json.Unmarshal(b, &f); err != nil {
		return 0, fmt.Errorf("%s: corrupt cache file, ignoring: %w", c.path, err)
	}
	if f.Version != cacheFormatVersion {
		return 0, nil
	}
	now := time.Now()
	n := 0
	c.mu.Lock()
	for k, e := range f.Entries {
		if now.Before(e.Exp) {
			c.m[k] = e
			n++
		}
	}
	c.mu.Unlock()
	return n, nil
}

// Save atomically writes non-expired entries to disk (temp file + rename),
// and is a no-op if nothing changed since the last save.
func (c *TTLCache[V]) Save() error {
	if c.path == "" {
		return nil
	}
	now := time.Now()
	c.mu.Lock()
	if !c.dirty {
		c.mu.Unlock()
		return nil
	}
	f := cacheFile[V]{Version: cacheFormatVersion, SavedAt: now, Entries: make(map[string]cacheEntry[V], len(c.m))}
	for k, e := range c.m {
		if now.Before(e.Exp) {
			f.Entries[k] = e
		}
	}
	c.dirty = false
	c.mu.Unlock()

	b, err := json.Marshal(f)
	if err == nil {
		err = writeFileAtomic(c.path, b)
	}
	if err != nil {
		c.mu.Lock()
		c.dirty = true
		c.mu.Unlock()
	}
	return err
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
