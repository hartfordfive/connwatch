// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

// Resolver does reverse-DNS (PTR) lookups for remote addresses, backed by a
// TTL cache that is persisted to disk so restarts don't repeat lookups.
//
// Lookups for new addresses run in parallel with a per-lookup timeout and an
// overall budget per poll, so a slow DNS server can never stall the poller for
// long. Addresses that did not finish inside the budget are retried on the
// next poll.
type Resolver struct {
	enabled bool
	ttl     time.Duration
	negTTL  time.Duration
	timeout time.Duration
	budget  time.Duration
	workers int
	metrics *Metrics
	cache   *TTLCache[string] // "" value = negative result
}

func NewResolver(enabled bool, ttl, negTTL, timeout time.Duration, workers int, cache *TTLCache[string], m *Metrics) *Resolver {
	if workers < 1 {
		workers = 1
	}
	return &Resolver{
		enabled: enabled,
		ttl:     ttl,
		negTTL:  negTTL,
		timeout: timeout,
		budget:  3 * time.Second,
		workers: workers,
		metrics: m,
		cache:   cache,
	}
}

// Lookup returns the cached hostname for ip ("" if unknown / not resolvable).
func (r *Resolver) Lookup(ip string) string {
	if !r.enabled || ip == "" {
		return ""
	}
	if isLoopback(ip) {
		return "localhost"
	}
	h, _ := r.cache.Peek(ip)
	return h
}

// ResolveBatch makes sure every address in ips has a fresh cache entry.
func (r *Resolver) ResolveBatch(ctx context.Context, ips []string) {
	if !r.enabled {
		return
	}
	seen := make(map[string]struct{}, len(ips))
	var need []string
	for _, ip := range ips {
		if ip == "" || ip == "*" || isLoopback(ip) {
			continue
		}
		if _, dup := seen[ip]; dup {
			continue
		}
		seen[ip] = struct{}{}
		if _, ok := r.cache.Get(ip); ok {
			continue
		}
		need = append(need, ip)
	}

	if len(need) > 0 {
		bctx, cancel := context.WithTimeout(ctx, r.budget)
		defer cancel()

		sem := make(chan struct{}, r.workers)
		var wg sync.WaitGroup
	loop:
		for _, ip := range need {
			select {
			case sem <- struct{}{}:
			case <-bctx.Done():
				break loop
			}
			wg.Add(1)
			go func(ip string) {
				defer wg.Done()
				defer func() { <-sem }()
				r.lookupOne(bctx, ip)
			}(ip)
		}
		wg.Wait()
	}
}

func (r *Resolver) lookupOne(bctx context.Context, ip string) {
	lctx, cancel := context.WithTimeout(bctx, r.timeout)
	defer cancel()

	names, err := net.DefaultResolver.LookupAddr(lctx, ip)
	if bctx.Err() != nil {
		return // poll budget exhausted: don't cache, retry next poll
	}
	if err != nil || len(names) == 0 {
		var de *net.DNSError
		if lctx.Err() != nil || (errors.As(err, &de) && de.IsTimeout) {
			r.metrics.dnsTimeout.Add(1)
		} else {
			r.metrics.dnsFailure.Add(1)
		}
		r.cache.Set(ip, "", r.negTTL)
		return
	}
	r.metrics.dnsSuccess.Add(1)
	r.cache.Set(ip, strings.TrimSuffix(names[0], "."), r.ttl)
}
