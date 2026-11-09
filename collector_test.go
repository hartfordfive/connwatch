// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
	"time"
)

// Real nettop output shape (columns come back in nettop's own order).
const sample = `,interface,state,bytes_in,bytes_out,
apsd.373,,,145075,275054,
tcp4 192.168.68.74:62492<->17.57.147.6:5223,en0,Established,145075,275054,
Google Chrome H.1102,,,9000,700,
tcp6 2001:db8::5.51265<->2606:4700::1111.443,en0,SynSent,0,0,
quic4 192.168.68.74:61891<->17.248.209.62:443,en0,,5811,6262,
tcp4 *:8080<->*:*,,Listen,,,
2BUA8C4S2C.com..2526,,,0,0,
udp6 fe80::1%en0.5353<->*.*,en0,,10,20,
weird, name.77,,,1,2,
tcp4 127.0.0.1:8080<->127.0.0.1:50000,lo0,FinWait1,1,2,
`

func TestParseNettop(t *testing.T) {
	raws, err := parseNettop(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if len(raws) != 6 {
		t.Fatalf("want 6 sockets, got %d: %+v", len(raws), raws)
	}
	a := raws[0]
	if a.PID != 373 || a.Command != "apsd" || a.Proto != "tcp" || a.Family != "IPv4" ||
		a.LocalAddr != "192.168.68.74" || a.LocalPort != 62492 || a.RemoteAddr != "17.57.147.6" ||
		a.RemotePort != 5223 || a.State != "ESTABLISHED" || a.Interface != "en0" ||
		a.BytesIn != 145075 || a.BytesOut != 275054 {
		t.Errorf("bad tcp4 parse: %+v", a)
	}
	b := raws[1]
	if b.PID != 1102 || b.Command != "Google Chrome H" || b.Family != "IPv6" || b.LocalAddr != "2001:db8::5" ||
		b.LocalPort != 51265 || b.RemoteAddr != "2606:4700::1111" || b.RemotePort != 443 || b.State != "SYN_SENT" {
		t.Errorf("bad tcp6 parse: %+v", b)
	}
	if q := raws[2]; q.Proto != "quic" || q.RemotePort != 443 || q.BytesIn != 5811 || q.State != "" {
		t.Errorf("bad quic parse: %+v", q)
	}
	if l := raws[3]; l.LocalAddr != "*" || l.LocalPort != 8080 || l.RemoteAddr != "" || l.State != "LISTEN" {
		t.Errorf("bad listener parse: %+v", l)
	}
	if u := raws[4]; u.PID != 2526 || u.Command != "2BUA8C4S2C.com." || u.LocalAddr != "fe80::1" || u.LocalPort != 5353 || u.RemoteAddr != "" {
		t.Errorf("bad dotted-name / udp6 wildcard parse: %+v", u)
	}
	if w := raws[5]; w.PID != 77 || w.Command != "weird, name" || w.State != "FIN_WAIT_1" || w.Interface != "lo0" {
		t.Errorf("bad comma-name parse: %+v", w)
	}
}

func TestParseNettopBadHeader(t *testing.T) {
	if _, err := parseNettop(strings.NewReader("garbage\n")); err == nil {
		t.Error("expected error for unexpected header")
	}
	if _, err := parseNettop(strings.NewReader("")); err == nil {
		t.Error("expected error for empty output")
	}
}

func TestTCPState(t *testing.T) {
	for in, want := range map[string]string{"Established": "ESTABLISHED", "SynSent": "SYN_SENT", "FinWait2": "FIN_WAIT_2",
		"CloseWait": "CLOSE_WAIT", "TimeWait": "TIME_WAIT", "": ""} {
		if got := tcpState(in); got != want {
			t.Errorf("tcpState(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParsePS(t *testing.T) {
	m := parsePS("    1 root             /sbin/launchd\n 1102 alain            /Applications/Google Chrome.app/Contents/MacOS/Google Chrome Helper (Aperitif)\njunk\n")
	if m[1].name != "launchd" || m[1].user != "root" {
		t.Errorf("bad: %+v", m[1])
	}
	if m[1102].name != "Google Chrome Helper (Aperitif)" || m[1102].user != "alain" {
		t.Errorf("bad: %+v", m[1102])
	}
}

func TestBuildConnsDirectionAndFilters(t *testing.T) {
	raws, _ := parseNettop(strings.NewReader(sample))
	// a listener on 8080 owned by pid 77 makes its 8080 connection inbound
	raws = append(raws, rawConn{PID: 77, Command: "weird, name", Proto: "tcp", State: "LISTEN", LocalAddr: "*", LocalPort: 8080})
	conns := buildConns(raws, Options{})
	dirs := map[string]string{}
	for _, c := range conns {
		dirs[c.Process+":"+c.RemoteAddr] = c.Direction
		if c.Direction == "listen" {
			t.Errorf("listening sockets should be hidden by default: %+v", c)
		}
	}
	if dirs["weird, name:127.0.0.1"] != "inbound" {
		t.Errorf("expected inbound, got %v", dirs)
	}
	if dirs["apsd:17.57.147.6"] != "outbound" {
		t.Errorf("expected outbound, got %v", dirs)
	}
	if n := len(buildConns(raws, Options{IncludeListening: true})); n <= len(conns) {
		t.Errorf("IncludeListening should add sockets")
	}
	for _, c := range buildConns(raws, Options{IgnoreLoopback: true}) {
		if c.RemoteAddr == "127.0.0.1" {
			t.Errorf("loopback not filtered: %+v", c)
		}
	}
}

func TestTrackerLifecycleAndBytes(t *testing.T) {
	tr := NewTracker()
	t0 := time.Now()
	c := Conn{ID: "x", Process: "curl", Proto: "tcp", State: "SYN_SENT", Direction: "outbound", RemoteAddr: "1.1.1.1", RemotePort: 443}

	ev, tf := tr.Update(t0, []Conn{c})
	if len(ev) != 1 || ev[0].Type != "open" || len(tf) != 0 {
		t.Fatalf("want open and no traffic, got %+v %+v", ev, tf)
	}

	c.State, c.RemoteHost, c.BytesIn, c.BytesOut = "ESTABLISHED", "one.one.one.one", 4000, 1000
	ev, tf = tr.Update(t0.Add(2*time.Second), []Conn{c})
	if len(ev) != 1 || ev[0].Type != "update" || ev[0].PrevState != "SYN_SENT" {
		t.Fatalf("want update, got %+v", ev)
	}
	if len(tf) != 1 || tf[0].DeltaIn != 4000 || tf[0].DeltaOut != 1000 || tf[0].Conn.RateIn != 2000 || tf[0].Conn.RateOut != 500 {
		t.Fatalf("bad traffic: %+v %+v", tf, tf[0].Conn)
	}

	// bytes-only change: traffic but no lifecycle event
	c.BytesIn = 5000
	ev, tf = tr.Update(t0.Add(3*time.Second), []Conn{c})
	if len(ev) != 0 || len(tf) != 1 || tf[0].DeltaIn != 1000 || tf[0].DeltaOut != 0 {
		t.Fatalf("want traffic only, got %+v %+v", ev, tf)
	}
	if st := tr.Stats(t0); st.BytesIn != 5000 || st.BytesOut != 1000 || st.RateIn != 1000 {
		t.Fatalf("bad stats: %+v", st)
	}

	ev, _ = tr.Update(t0.Add(5*time.Second), nil)
	if len(ev) != 1 || ev[0].Type != "close" || ev[0].DurationMS != 5000 || ev[0].Conn.BytesIn != 5000 {
		t.Fatalf("want close after 5s with final bytes, got %+v", ev)
	}
	if a, o, cl := tr.Counts(); a != 0 || o != 1 || cl != 1 {
		t.Errorf("counts wrong: %d %d %d", a, o, cl)
	}
}

func TestCounterReset(t *testing.T) {
	if counterDelta(100, 150) != 50 || counterDelta(100, 30) != 30 {
		t.Error("counterDelta wrong")
	}
}
