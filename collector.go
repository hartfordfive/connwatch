// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// rawConn is one socket as reported by nettop, before filtering and enrichment.
type rawConn struct {
	PID        int
	Command    string
	User       string
	Proto      string // tcp, udp or quic
	Family     string // IPv4 or IPv6
	State      string // e.g. ESTABLISHED, LISTEN (TCP only)
	Interface  string // e.g. en0, utun4
	LocalAddr  string // may be "*" for wildcard binds
	LocalPort  int
	RemoteAddr string // empty for listening / unconnected sockets
	RemotePort int
	BytesIn    uint64 // cumulative bytes received on this socket
	BytesOut   uint64 // cumulative bytes sent on this socket
}

// Collector enumerates sockets and their byte counters by running macOS's
// nettop, which reads the kernel's per-flow network statistics.
type Collector struct {
	path    string
	timeout time.Duration
	procs   *procTable
}

func NewCollector(path string, timeout time.Duration) (*Collector, error) {
	if path == "" {
		p, err := exec.LookPath("nettop")
		if err != nil {
			p = "/usr/bin/nettop"
			if _, serr := exec.LookPath(p); serr != nil {
				return nil, fmt.Errorf("nettop not found (this tool needs macOS): %w", err)
			}
		}
		path = p
	}
	return &Collector{path: path, timeout: timeout, procs: newProcTable()}, nil
}

// Collect takes one nettop snapshot.
//
//	-L 1  one sample, then exit (values are totals since each socket opened)
//	-n    no DNS lookups (we do our own)
//	-x    exact numbers, not 1.2 MiB
//	-J    only these columns
func (c *Collector) Collect(ctx context.Context) ([]rawConn, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.path, "-L", "1", "-n", "-x", "-J", "interface,state,bytes_in,bytes_out")
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("nettop: %w: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, fmt.Errorf("nettop: %w", err)
	}
	raws, err := parseNettop(strings.NewReader(string(out)))
	if err != nil {
		return nil, err
	}
	c.procs.fill(ctx, raws)
	return raws, nil
}

var socketPrefixes = []string{"tcp4 ", "tcp6 ", "udp4 ", "udp6 ", "quic4 ", "quic6 "}

func socketLine(s string) bool {
	for _, p := range socketPrefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// parseNettop parses `nettop -x -J ...` CSV output:
//
//	,interface,state,bytes_in,bytes_out,
//	apsd.373,,,145075,275054,
//	tcp4 192.168.68.74:62492<->17.57.147.6:5223,en0,Established,145075,275054,
//	tcp6 fe80::1%en0.51265<->fe80::2%en0.57353,en0,Established,73539,27389,
//
// A process line ("name.pid") is followed by that process's sockets. The
// column order comes from the header, not from the order requested.
func parseNettop(r io.Reader) ([]rawConn, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	col := map[string]int{}
	ncols := 0
	var (
		out  []rawConn
		pid  int
		name string
	)
	get := func(f []string, k string) string {
		if i, ok := col[k]; ok && i < len(f) {
			return f[i]
		}
		return ""
	}

	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" {
			continue
		}
		f := strings.Split(line, ",")
		if ncols == 0 { // header
			if f[0] != "" && f[0] != "time" {
				return nil, fmt.Errorf("nettop: unexpected header %q", line)
			}
			for i, h := range f {
				if h != "" {
					col[h] = i
				}
			}
			ncols = len(f)
			continue
		}
		// A process name containing commas splits into extra fields: glue them back.
		if extra := len(f) - ncols; extra > 0 {
			f = append([]string{strings.Join(f[:extra+1], ",")}, f[extra+1:]...)
		}

		if !socketLine(f[0]) {
			// "Google Chrome H.1102" -> name, pid (names may contain dots)
			i := strings.LastIndexByte(f[0], '.')
			if i < 0 {
				pid, name = 0, f[0]
				continue
			}
			pid, _ = strconv.Atoi(f[0][i+1:])
			name = f[0][:i]
			continue
		}
		if pid == 0 {
			continue
		}
		rc, ok := parseSocket(f[0])
		if !ok {
			continue
		}
		rc.PID, rc.Command = pid, name
		rc.Interface = get(f, "interface")
		rc.State = tcpState(get(f, "state"))
		rc.BytesIn, _ = strconv.ParseUint(get(f, "bytes_in"), 10, 64)
		rc.BytesOut, _ = strconv.ParseUint(get(f, "bytes_out"), 10, 64)
		out = append(out, rc)
	}
	if ncols == 0 && sc.Err() == nil {
		return nil, errors.New("nettop: empty output")
	}
	return out, sc.Err()
}

// parseSocket parses "tcp4 1.2.3.4:443<->5.6.7.8:9" or "udp6 fe80::1%en0.5353<->*.*".
func parseSocket(s string) (rawConn, bool) {
	sp := strings.IndexByte(s, ' ')
	if sp < 0 {
		return rawConn{}, false
	}
	kind, addrs := s[:sp], s[sp+1:]
	rc := rawConn{Proto: kind[:len(kind)-1], Family: "IPv4"}
	sep := byte(':')
	if kind[len(kind)-1] == '6' {
		rc.Family, sep = "IPv6", '.'
	}
	local, remote, found := strings.Cut(addrs, "<->")
	if !found {
		return rawConn{}, false
	}
	var ok bool
	if rc.LocalAddr, rc.LocalPort, ok = splitHostPort(local, sep); !ok {
		return rawConn{}, false
	}
	ra, rp, ok := splitHostPort(remote, sep)
	if !ok {
		return rawConn{}, false
	}
	if ra != "*" && ra != "" {
		rc.RemoteAddr, rc.RemotePort = ra, rp
	}
	return rc, true
}

func splitHostPort(s string, sep byte) (host string, port int, ok bool) {
	i := strings.LastIndexByte(s, sep)
	if i < 0 {
		return "", 0, false
	}
	host, ps := s[:i], s[i+1:]
	if z := strings.IndexByte(host, '%'); z >= 0 { // drop IPv6 zone, e.g. fe80::1%en0
		host = host[:z]
	}
	if ps != "*" {
		p, err := strconv.Atoi(ps)
		if err != nil {
			return "", 0, false
		}
		port = p
	}
	return host, port, true
}

// tcpState turns nettop's "SynSent" / "FinWait1" into "SYN_SENT" / "FIN_WAIT_1".
func tcpState(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (unicode.IsUpper(r) || (unicode.IsDigit(r) && !unicode.IsDigit(rune(s[i-1])))) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToUpper(r))
	}
	return b.String()
}

// procTable maps pids to their full process name and user. nettop truncates
// names to 15 characters and doesn't report the user, so we ask ps, but only
// when a pid we haven't seen (or a reused pid) shows up.
type procTable struct {
	mu sync.Mutex
	m  map[int]procInfo
}

type procInfo struct {
	trunc string // name as nettop showed it, used to detect pid reuse
	name  string
	user  string
}

func newProcTable() *procTable { return &procTable{m: map[int]procInfo{}} }

func (t *procTable) fill(ctx context.Context, raws []rawConn) {
	t.mu.Lock()
	defer t.mu.Unlock()

	need := false
	live := map[int]string{}
	for _, r := range raws {
		live[r.PID] = r.Command
		if p, ok := t.m[r.PID]; !ok || p.trunc != r.Command {
			need = true
		}
	}
	if need {
		if ps, err := runPS(ctx); err == nil {
			for pid, trunc := range live {
				if p, ok := ps[pid]; ok {
					p.trunc = trunc
					t.m[pid] = p
				} else if _, ok := t.m[pid]; !ok {
					t.m[pid] = procInfo{trunc: trunc, name: trunc} // exited already: keep nettop's name
				}
			}
		}
	}
	for pid := range t.m {
		if _, ok := live[pid]; !ok {
			delete(t.m, pid)
		}
	}
	for i := range raws {
		if p, ok := t.m[raws[i].PID]; ok {
			raws[i].Command, raws[i].User = p.name, p.user
		}
	}
}

func runPS(ctx context.Context) (map[int]procInfo, error) {
	out, err := exec.CommandContext(ctx, "/bin/ps", "-axo", "pid=,user=,comm=").Output()
	if err != nil {
		return nil, err
	}
	return parsePS(string(out)), nil
}

// parsePS parses "  1102 alain  /Applications/.../Google Chrome Helper (Aperitif)".
func parsePS(s string) map[int]procInfo {
	m := map[int]procInfo{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		pidS, rest, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		pid, err := strconv.Atoi(pidS)
		if err != nil {
			continue
		}
		rest = strings.TrimLeft(rest, " ")
		user, comm, ok := strings.Cut(rest, " ")
		if !ok {
			continue
		}
		comm = strings.TrimSpace(comm)
		m[pid] = procInfo{name: filepath.Base(comm), user: user}
	}
	return m
}
