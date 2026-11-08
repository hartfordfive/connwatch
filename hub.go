// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"sync"
)

type subscriber struct {
	ch chan []byte
}

// Hub fans out Server-Sent-Events frames to all connected browsers.
type Hub struct {
	mu      sync.Mutex
	subs    map[*subscriber]struct{}
	closed  bool
	metrics *Metrics
}

func NewHub(m *Metrics) *Hub {
	return &Hub{subs: make(map[*subscriber]struct{}), metrics: m}
}

func (h *Hub) Subscribe() *subscriber {
	s := &subscriber{ch: make(chan []byte, 512)}
	h.mu.Lock()
	if h.closed {
		close(s.ch)
	} else {
		h.subs[s] = struct{}{}
		h.metrics.sseClients.Store(int64(len(h.subs)))
	}
	h.mu.Unlock()
	return s
}

func (h *Hub) Unsubscribe(s *subscriber) {
	h.mu.Lock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.ch)
		h.metrics.sseClients.Store(int64(len(h.subs)))
	}
	h.mu.Unlock()
}

// Publish sends one named SSE event with a JSON payload to every subscriber.
// A subscriber whose buffer is full is dropped; the browser reconnects and
// receives a fresh snapshot.
func (h *Hub) Publish(event string, v any) {
	frame, err := encodeSSE(event, v)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		select {
		case s.ch <- frame:
		default:
			delete(h.subs, s)
			close(s.ch)
			h.metrics.sseDropped.Add(1)
		}
	}
	h.metrics.sseClients.Store(int64(len(h.subs)))
}

func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for s := range h.subs {
		delete(h.subs, s)
		close(s.ch)
	}
	h.metrics.sseClients.Store(0)
}

func encodeSSE(event string, v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", event, data)), nil
}
