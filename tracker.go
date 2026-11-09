// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net"
	"sort"
	"sync"
	"time"
)

// Conn is a tracked network connection.
type Conn struct {
	ID         string    `json:"id"`
	PID        int       `json:"pid"`
	Process    string    `json:"process"`
	User       string    `json:"user,omitempty"`
	Proto      string    `json:"proto"` // tcp | udp | quic
	Family     string    `json:"family,omitempty"`
	State      string    `json:"state,omitempty"`
	Direction  string    `json:"direction"` // outbound | inbound | listen
	Interface  string    `json:"interface,omitempty"`
	LocalAddr  string    `json:"local_addr"`
	LocalPort  int       `json:"local_port"`
	RemoteAddr string    `json:"remote_addr,omitempty"`
	RemotePort int       `json:"remote_port,omitempty"`
	RemoteHost string    `json:"remote_host,omitempty"` // reverse-DNS name of RemoteAddr
	Geo        *GeoInfo  `json:"geo,omitempty"`         // GeoIP data for RemoteAddr (public IPs only)
	BytesIn    uint64    `json:"bytes_in"`              // received, cumulative
	BytesOut   uint64    `json:"bytes_out"`             // sent, cumulative
	RateIn     float64   `json:"rate_in"`               // bytes/s over the last poll
	RateOut    float64   `json:"rate_out"`              // bytes/s over the last poll
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
}

// Event describes a change between two polls.
type Event struct {
	Type       string    `json:"type"` // open | update | close
	Time       time.Time `json:"time"`
	Conn       Conn      `json:"conn"`
	PrevState  string    `json:"prev_state,omitempty"`
	DurationMS int64     `json:"duration_ms,omitempty"`
}

// Traffic is the byte movement of one connection during one poll. Byte
// counter changes are reported this way rather than as "update" events, so
// the log and the event feed aren't flooded every poll.
type Traffic struct {
	Conn     *Conn  // snapshot (labels for metrics)
	DeltaIn  uint64 // bytes received since the previous poll
	DeltaOut uint64 // bytes sent since the previous poll
}

// Stats is a small summary sent after every poll.
type Stats struct {
	Time        time.Time `json:"time"`
	Active      int       `json:"active"`
	OpenedTotal uint64    `json:"opened_total"`
	ClosedTotal uint64    `json:"closed_total"`
	PollMS      float64   `json:"poll_ms"`
	RateIn      float64   `json:"rate_in"`  // bytes/s, all connections
	RateOut     float64   `json:"rate_out"` // bytes/s, all connections
	BytesIn     uint64    `json:"bytes_in_total"`
	BytesOut    uint64    `json:"bytes_out_total"`
}

type Options struct {
	IncludeListening bool
	IgnoreLoopback   bool
}

func connID(r rawConn) string {
	key := fmt.Sprintf("%d|%s|%s|%d|%s|%d", r.PID, r.Proto, r.LocalAddr, r.LocalPort, r.RemoteAddr, r.RemotePort)
	h := fnv.New64a()
	h.Write([]byte(key))
	return hex.EncodeToString(h.Sum(nil))
}

func isLoopback(addr string) bool {
	ip := net.ParseIP(addr)
	return ip != nil && ip.IsLoopback()
}

// buildConns turns collector rows into Conns: it merges duplicates (one socket
// listed more than once), classifies direction, and applies the filters.
func buildConns(raws []rawConn, opt Options) []Conn {
	type lk struct {
		pid   int
		proto string
		port  int
	}
	listening := make(map[lk]struct{})
	for _, r := range raws {
		if r.RemoteAddr == "" {
			listening[lk{r.PID, r.Proto, r.LocalPort}] = struct{}{}
		}
	}

	idx := make(map[string]int, len(raws))
	out := make([]Conn, 0, len(raws))
	for _, r := range raws {
		dir := "outbound"
		switch {
		case r.RemoteAddr == "":
			dir = "listen"
		default:
			if _, ok := listening[lk{r.PID, r.Proto, r.LocalPort}]; ok {
				dir = "inbound"
			}
		}
		if dir == "listen" && !opt.IncludeListening {
			continue
		}
		if opt.IgnoreLoopback && r.RemoteAddr != "" && isLoopback(r.RemoteAddr) {
			continue
		}
		id := connID(r)
		if i, dup := idx[id]; dup {
			out[i].BytesIn += r.BytesIn
			out[i].BytesOut += r.BytesOut
			continue
		}
		idx[id] = len(out)
		out = append(out, Conn{
			ID:         id,
			PID:        r.PID,
			Process:    r.Command,
			User:       r.User,
			Proto:      r.Proto,
			Family:     r.Family,
			State:      r.State,
			Direction:  dir,
			Interface:  r.Interface,
			LocalAddr:  r.LocalAddr,
			LocalPort:  r.LocalPort,
			RemoteAddr: r.RemoteAddr,
			RemotePort: r.RemotePort,
			BytesIn:    r.BytesIn,
			BytesOut:   r.BytesOut,
		})
	}
	return out
}

// Tracker keeps the set of active connections and diffs successive polls.
type Tracker struct {
	mu       sync.RWMutex
	active   map[string]*Conn
	opened   uint64
	closed   uint64
	bytesIn  uint64 // all bytes observed since start
	bytesOut uint64
	rateIn   float64
	rateOut  float64
}

func NewTracker() *Tracker {
	return &Tracker{active: make(map[string]*Conn)}
}

// counterDelta handles a counter that went backwards (socket reused) as a reset.
func counterDelta(prev, cur uint64) uint64 {
	if cur >= prev {
		return cur - prev
	}
	return cur
}

// Update applies one poll's worth of connections. It returns lifecycle events
// and, separately, the bytes each connection moved since the previous poll.
func (t *Tracker) Update(now time.Time, conns []Conn) ([]Event, []Traffic) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var (
		events  []Event
		traffic []Traffic
		rIn     float64
		rOut    float64
	)
	seen := make(map[string]struct{}, len(conns))

	for _, c := range conns {
		seen[c.ID] = struct{}{}
		cur, ok := t.active[c.ID]
		if !ok {
			c.FirstSeen, c.LastSeen = now, now
			cc := c
			t.active[c.ID] = &cc
			t.opened++
			events = append(events, Event{Type: "open", Time: now, Conn: cc})
			if cc.BytesIn > 0 || cc.BytesOut > 0 {
				traffic = append(traffic, Traffic{Conn: &cc, DeltaIn: cc.BytesIn, DeltaOut: cc.BytesOut})
				t.bytesIn += cc.BytesIn
				t.bytesOut += cc.BytesOut
			}
			continue
		}

		dIn, dOut := counterDelta(cur.BytesIn, c.BytesIn), counterDelta(cur.BytesOut, c.BytesOut)
		if dt := now.Sub(cur.LastSeen).Seconds(); dt > 0 {
			cur.RateIn, cur.RateOut = float64(dIn)/dt, float64(dOut)/dt
		}
		cur.BytesIn, cur.BytesOut = c.BytesIn, c.BytesOut
		cur.LastSeen = now
		rIn += cur.RateIn
		rOut += cur.RateOut
		if dIn > 0 || dOut > 0 {
			snap := *cur
			traffic = append(traffic, Traffic{Conn: &snap, DeltaIn: dIn, DeltaOut: dOut})
			t.bytesIn += dIn
			t.bytesOut += dOut
		}

		prev := cur.State
		changed := false
		if c.State != cur.State {
			cur.State = c.State
			changed = true
		}
		if c.Interface != "" && c.Interface != cur.Interface {
			cur.Interface = c.Interface
		}
		if cur.RemoteHost == "" && c.RemoteHost != "" {
			cur.RemoteHost = c.RemoteHost
			changed = true
		}
		if cur.Geo == nil && c.Geo != nil {
			cur.Geo = c.Geo
			changed = true
		}
		if changed {
			events = append(events, Event{Type: "update", Time: now, Conn: *cur, PrevState: prev})
		}
	}
	t.rateIn, t.rateOut = rIn, rOut

	var closing []*Conn
	for id, cur := range t.active {
		if _, ok := seen[id]; !ok {
			closing = append(closing, cur)
			delete(t.active, id)
		}
	}
	sort.Slice(closing, func(i, j int) bool { return closing[i].FirstSeen.Before(closing[j].FirstSeen) })
	for _, cur := range closing {
		t.closed++
		cur.RateIn, cur.RateOut = 0, 0
		events = append(events, Event{
			Type:       "close",
			Time:       now,
			Conn:       *cur,
			DurationMS: now.Sub(cur.FirstSeen).Milliseconds(),
		})
	}
	return events, traffic
}

// Snapshot returns a copy of all active connections, newest first.
func (t *Tracker) Snapshot() []Conn {
	t.mu.RLock()
	out := make([]Conn, 0, len(t.active))
	for _, c := range t.active {
		out = append(out, *c)
	}
	t.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].FirstSeen.Equal(out[j].FirstSeen) {
			return out[i].FirstSeen.After(out[j].FirstSeen)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (t *Tracker) Counts() (active int, opened, closed uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.active), t.opened, t.closed
}

// Stats returns the counters plus current throughput.
func (t *Tracker) Stats(now time.Time) Stats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return Stats{
		Time: now, Active: len(t.active), OpenedTotal: t.opened, ClosedTotal: t.closed,
		RateIn: t.rateIn, RateOut: t.rateOut, BytesIn: t.bytesIn, BytesOut: t.bytesOut,
	}
}
