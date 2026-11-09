// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// ConnLog appends one JSON object per connection event to a file (JSON Lines).
// Send SIGHUP to the process to reopen the file after log rotation.
type ConnLog struct {
	path    string
	metrics *Metrics

	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

type logRecord struct {
	TS         string  `json:"ts"`
	Event      string  `json:"event"` // open | update | close
	PID        int     `json:"pid"`
	Process    string  `json:"process"`
	User       string  `json:"user,omitempty"`
	Proto      string  `json:"proto"`
	Family     string  `json:"family,omitempty"`
	State      string  `json:"state,omitempty"`
	PrevState  string  `json:"prev_state,omitempty"`
	Direction  string  `json:"direction"`
	SrcIP      string  `json:"src_ip"`
	SrcPort    int     `json:"src_port"`
	DstIP      string  `json:"dst_ip,omitempty"`
	DstPort    int     `json:"dst_port,omitempty"`
	DstHost    string  `json:"dst_host,omitempty"`
	DstCC      string  `json:"dst_country,omitempty"`
	DstCountry string  `json:"dst_country_name,omitempty"`
	DstCity    string  `json:"dst_city,omitempty"`
	DstLat     float64 `json:"dst_lat,omitempty"`
	DstLon     float64 `json:"dst_lon,omitempty"`
	DstASN     uint    `json:"dst_asn,omitempty"`
	DstOrg     string  `json:"dst_org,omitempty"`
	BytesIn    uint64  `json:"bytes_in"`
	BytesOut   uint64  `json:"bytes_out"`
	Interface  string  `json:"interface,omitempty"`
	DurationMS int64   `json:"duration_ms,omitempty"`
}

func NewConnLog(path string, m *Metrics) (*ConnLog, error) {
	l := &ConnLog{path: path, metrics: m}
	if err := l.Reopen(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *ConnLog) Reopen() error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		_ = l.f.Close()
	}
	l.f = f
	l.enc = json.NewEncoder(f)
	return nil
}

func (l *ConnLog) Write(ev Event) {
	rec := logRecord{
		TS:         ev.Time.UTC().Format(time.RFC3339Nano),
		Event:      ev.Type,
		PID:        ev.Conn.PID,
		Process:    ev.Conn.Process,
		User:       ev.Conn.User,
		Proto:      ev.Conn.Proto,
		Family:     ev.Conn.Family,
		State:      ev.Conn.State,
		PrevState:  ev.PrevState,
		Direction:  ev.Conn.Direction,
		SrcIP:      ev.Conn.LocalAddr,
		SrcPort:    ev.Conn.LocalPort,
		DstIP:      ev.Conn.RemoteAddr,
		DstPort:    ev.Conn.RemotePort,
		DstHost:    ev.Conn.RemoteHost,
		BytesIn:    ev.Conn.BytesIn,
		BytesOut:   ev.Conn.BytesOut,
		Interface:  ev.Conn.Interface,
		DurationMS: ev.DurationMS,
	}
	if g := ev.Conn.Geo; g != nil {
		rec.DstCC, rec.DstCountry, rec.DstCity = g.CountryCode, g.Country, g.City
		rec.DstLat, rec.DstLon, rec.DstASN, rec.DstOrg = g.Lat, g.Lon, g.ASN, g.Org
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.enc.Encode(rec); err != nil { // Encode appends the newline
		l.metrics.logErrors.Add(1)
		return
	}
	l.metrics.logWritten.Add(1)
}

func (l *ConnLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}
