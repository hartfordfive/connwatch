// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const metricPrefix = "connwatch_"

var durationBounds = []float64{1, 5, 15, 60, 300, 900, 3600, 14400}

const (
	maxProcessLabels = 100
	maxHostLabels    = 500
	maxASNLabels     = 100
)

// Metrics is a tiny, dependency-free Prometheus text-format exporter.
type Metrics struct {
	start   time.Time
	version string
	byHost  bool

	polls        atomic.Uint64
	pollErrors   atomic.Uint64
	pollNanos    atomic.Uint64
	lastPollUnix atomic.Int64 // unix nanoseconds

	dnsSuccess      atomic.Uint64
	dnsFailure      atomic.Uint64
	dnsTimeout      atomic.Uint64
	dnsCache        *TTLCache[string]    // set by main
	geo             *GeoIP               // set by main; nil when GeoIP is off
	pollInterval    func() time.Duration // set by main
	pollPaused      func() bool          // set by main
	cacheSaves      atomic.Uint64
	cacheSaveErrors atomic.Uint64

	sseClients atomic.Int64
	sseDropped atomic.Uint64

	logWritten atomic.Uint64
	logErrors  atomic.Uint64

	mu            sync.Mutex
	opened        map[[2]string]uint64 // proto, direction
	closed        map[[2]string]uint64
	durBuckets    map[string][]uint64 // proto -> per-bucket (non-cumulative) counts, last = +Inf
	durSum        map[string]float64
	durCount      map[string]uint64
	hostOpened    map[string]uint64
	activeStates  map[[3]string]int // proto, state, direction
	activeProcs   map[string]int
	bytesProto    *cappedCounter // proto
	bytesProc     *cappedCounter // process
	bytesCountry  *cappedCounter // country
	bytesASN      *cappedCounter // "asn\x00org"
	activeCountry map[string]int
	activeASN     map[string]int // "AS15169 Google LLC" -> n
	uniqueRemotes int
}

func NewMetrics(version string, byHost bool) *Metrics {
	return &Metrics{
		start:        time.Now(),
		version:      version,
		byHost:       byHost,
		opened:       map[[2]string]uint64{},
		closed:       map[[2]string]uint64{},
		durBuckets:   map[string][]uint64{},
		durSum:       map[string]float64{},
		durCount:     map[string]uint64{},
		hostOpened:   map[string]uint64{},
		activeStates: map[[3]string]int{},
		activeProcs:  map[string]int{},
		bytesProto:   newCappedCounter(0, ""),
		bytesProc:    newCappedCounter(maxProcessLabels, "other"),
		bytesCountry: newCappedCounter(0, ""),
		bytesASN:     newCappedCounter(maxASNLabels, "0\x00other"),
	}
}

func (m *Metrics) ObservePoll(d time.Duration, err error) {
	m.polls.Add(1)
	m.pollNanos.Add(uint64(d))
	if err != nil {
		m.pollErrors.Add(1)
		return
	}
	m.lastPollUnix.Store(time.Now().UnixNano())
}

func (m *Metrics) ObserveEvent(ev Event) {
	c := ev.Conn
	key := [2]string{c.Proto, c.Direction}
	m.mu.Lock()
	defer m.mu.Unlock()
	switch ev.Type {
	case "open":
		m.opened[key]++
		if m.byHost && c.RemoteAddr != "" {
			h := c.RemoteHost
			if h == "" {
				h = c.RemoteAddr
			}
			if _, ok := m.hostOpened[h]; !ok && len(m.hostOpened) >= maxHostLabels {
				h = "other"
			}
			m.hostOpened[h]++
		}
	case "close":
		m.closed[key]++
		secs := float64(ev.DurationMS) / 1000
		b := m.durBuckets[c.Proto]
		if b == nil {
			b = make([]uint64, len(durationBounds)+1)
			m.durBuckets[c.Proto] = b
		}
		idx := len(durationBounds)
		for i, ub := range durationBounds {
			if secs <= ub {
				idx = i
				break
			}
		}
		b[idx]++
		m.durSum[c.Proto] += secs
		m.durCount[c.Proto]++
	}
}

// cappedCounter is a pair of byte counters (in, out) per label value. Once
// max distinct values exist, new ones are counted under the overflow label,
// keeping Prometheus cardinality bounded while staying monotonic.
type cappedCounter struct {
	max      int
	overflow string
	in, out  map[string]uint64
}

func newCappedCounter(max int, overflow string) *cappedCounter {
	return &cappedCounter{max: max, overflow: overflow, in: map[string]uint64{}, out: map[string]uint64{}}
}

func (c *cappedCounter) add(k string, in, out uint64) {
	if _, ok := c.in[k]; !ok && c.max > 0 && len(c.in) >= c.max {
		k = c.overflow
	}
	c.in[k] += in
	c.out[k] += out
}

// ObserveTraffic adds the bytes one connection moved during a poll.
func (m *Metrics) ObserveTraffic(t Traffic) {
	c := t.Conn
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bytesProto.add(c.Proto, t.DeltaIn, t.DeltaOut)
	m.bytesProc.add(c.Process, t.DeltaIn, t.DeltaOut)
	if c.Geo != nil {
		if c.Geo.CountryCode != "" {
			m.bytesCountry.add(c.Geo.CountryCode, t.DeltaIn, t.DeltaOut)
		}
		if c.Geo.ASN != 0 {
			m.bytesASN.add(fmt.Sprintf("%d\x00%s", c.Geo.ASN, c.Geo.Org), t.DeltaIn, t.DeltaOut)
		}
	}
}

func writeBytes(b *bytes.Buffer, name, help, label string, c *cappedCounter, split bool) {
	head(b, name, "counter", help)
	keys := make([]string, 0, len(c.in))
	for k := range c.in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, d := range []struct {
			dir string
			v   uint64
		}{{"in", c.in[k]}, {"out", c.out[k]}} {
			var l string
			if split {
				num, org, _ := strings.Cut(k, "\x00")
				l = lbl("asn", num, "org", org, "direction", d.dir)
			} else {
				l = lbl(label, k, "direction", d.dir)
			}
			fmt.Fprintf(b, "%s%s%s %d\n", metricPrefix, name, l, d.v)
		}
	}
}

// SetActive recomputes the "currently active" gauges from a snapshot.
func (m *Metrics) SetActive(conns []Conn) {
	states := map[[3]string]int{}
	procs := map[string]int{}
	remotes := map[string]struct{}{}
	countries := map[string]int{}
	asns := map[string]int{}
	for _, c := range conns {
		if c.Geo != nil {
			if c.Geo.CountryCode != "" {
				countries[c.Geo.CountryCode]++
			}
			if c.Geo.ASN != 0 {
				asns[fmt.Sprintf("%d\x00%s", c.Geo.ASN, c.Geo.Org)]++
			}
		}
		st := c.State
		if st == "" {
			st = "none"
		}
		states[[3]string{c.Proto, st, c.Direction}]++
		procs[c.Process]++
		if c.RemoteAddr != "" {
			remotes[c.RemoteAddr] = struct{}{}
		}
	}
	procs = capTop(procs, maxProcessLabels, "other")
	asns = capTop(asns, maxASNLabels, "0\x00other")
	m.mu.Lock()
	m.activeStates, m.activeProcs, m.uniqueRemotes = states, procs, len(remotes)
	m.activeCountry, m.activeASN = countries, asns
	m.mu.Unlock()
}

// capTop keeps the max-1 largest entries and folds the rest into other.
func capTop(in map[string]int, max int, other string) map[string]int {
	if len(in) <= max {
		return in
	}
	type kv struct {
		k string
		v int
	}
	all := make([]kv, 0, len(in))
	for k, v := range in {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].v != all[j].v {
			return all[i].v > all[j].v
		}
		return all[i].k < all[j].k
	})
	out := map[string]int{}
	for i, e := range all {
		if i < max-1 {
			out[e.k] = e.v
		} else {
			out[other] += e.v
		}
	}
	return out
}

func sortedStrKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func esc(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return strings.ReplaceAll(s, `"`, `\"`)
}

func lbl(kv ...string) string {
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, kv[i], esc(kv[i+1]))
	}
	b.WriteByte('}')
	return b.String()
}

func ff(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

func head(b *bytes.Buffer, name, typ, help string) {
	fmt.Fprintf(b, "# HELP %s%s %s\n# TYPE %s%s %s\n", metricPrefix, name, help, metricPrefix, name, typ)
}

func sortedKeys2(m map[[2]string]uint64) [][2]string {
	keys := make([][2]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	return keys
}

// Write renders all metrics in the Prometheus text exposition format.
func (m *Metrics) Write(w io.Writer) {
	var b bytes.Buffer
	p := metricPrefix

	head(&b, "build_info", "gauge", "Build information.")
	fmt.Fprintf(&b, "%sbuild_info%s 1\n", p, lbl("version", m.version, "go_version", runtime.Version()))

	head(&b, "start_time_seconds", "gauge", "Process start time, unix seconds.")
	fmt.Fprintf(&b, "%sstart_time_seconds %d\n", p, m.start.Unix())

	m.mu.Lock()
	// active gauges
	head(&b, "connections_active", "gauge", "Currently active connections by protocol, TCP state and direction.")
	skeys := make([][3]string, 0, len(m.activeStates))
	for k := range m.activeStates {
		skeys = append(skeys, k)
	}
	sort.Slice(skeys, func(i, j int) bool {
		for x := 0; x < 3; x++ {
			if skeys[i][x] != skeys[j][x] {
				return skeys[i][x] < skeys[j][x]
			}
		}
		return false
	})
	for _, k := range skeys {
		fmt.Fprintf(&b, "%sconnections_active%s %d\n", p, lbl("proto", k[0], "state", k[1], "direction", k[2]), m.activeStates[k])
	}

	head(&b, "connections_active_by_process", "gauge", "Currently active connections per process name (top 100).")
	pnames := make([]string, 0, len(m.activeProcs))
	for k := range m.activeProcs {
		pnames = append(pnames, k)
	}
	sort.Strings(pnames)
	for _, k := range pnames {
		fmt.Fprintf(&b, "%sconnections_active_by_process%s %d\n", p, lbl("process", k), m.activeProcs[k])
	}

	head(&b, "connections_active_by_country", "gauge", "Currently active connections per remote country (ISO code, GeoIP).")
	for _, k := range sortedStrKeys(m.activeCountry) {
		fmt.Fprintf(&b, "%sconnections_active_by_country%s %d\n", p, lbl("country", k), m.activeCountry[k])
	}
	head(&b, "connections_active_by_asn", "gauge", "Currently active connections per remote autonomous system (top 100, GeoIP).")
	for _, k := range sortedStrKeys(m.activeASN) {
		num, org, _ := strings.Cut(k, "\x00")
		fmt.Fprintf(&b, "%sconnections_active_by_asn%s %d\n", p, lbl("asn", num, "org", org), m.activeASN[k])
	}

	head(&b, "remote_addresses_active", "gauge", "Distinct remote IP addresses across active connections.")
	fmt.Fprintf(&b, "%sremote_addresses_active %d\n", p, m.uniqueRemotes)

	// counters
	head(&b, "connections_opened_total", "counter", "Connections first observed, by protocol and direction.")
	for _, k := range sortedKeys2(m.opened) {
		fmt.Fprintf(&b, "%sconnections_opened_total%s %d\n", p, lbl("proto", k[0], "direction", k[1]), m.opened[k])
	}
	head(&b, "connections_closed_total", "counter", "Connections that disappeared, by protocol and direction.")
	for _, k := range sortedKeys2(m.closed) {
		fmt.Fprintf(&b, "%sconnections_closed_total%s %d\n", p, lbl("proto", k[0], "direction", k[1]), m.closed[k])
	}

	// duration histogram
	head(&b, "connection_duration_seconds", "histogram", "Observed lifetime of closed connections (accurate to about one poll interval).")
	protos := make([]string, 0, len(m.durBuckets))
	for k := range m.durBuckets {
		protos = append(protos, k)
	}
	sort.Strings(protos)
	for _, pr := range protos {
		var cum uint64
		for i, ub := range durationBounds {
			cum += m.durBuckets[pr][i]
			fmt.Fprintf(&b, "%sconnection_duration_seconds_bucket%s %d\n", p, lbl("proto", pr, "le", ff(ub)), cum)
		}
		fmt.Fprintf(&b, "%sconnection_duration_seconds_bucket%s %d\n", p, lbl("proto", pr, "le", "+Inf"), m.durCount[pr])
		fmt.Fprintf(&b, "%sconnection_duration_seconds_sum%s %s\n", p, lbl("proto", pr), ff(m.durSum[pr]))
		fmt.Fprintf(&b, "%sconnection_duration_seconds_count%s %d\n", p, lbl("proto", pr), m.durCount[pr])
	}

	writeBytes(&b, "bytes_total", "Bytes received (in) and sent (out) on observed connections, by protocol.", "proto", m.bytesProto, false)
	writeBytes(&b, "bytes_by_process_total", "Bytes received / sent per process name (first 100 processes, then \"other\").", "process", m.bytesProc, false)
	writeBytes(&b, "bytes_by_country_total", "Bytes received / sent per remote country (GeoIP).", "country", m.bytesCountry, false)
	writeBytes(&b, "bytes_by_asn_total", "Bytes received / sent per remote autonomous system (first 100, then \"other\").", "", m.bytesASN, true)

	if m.byHost {
		head(&b, "remote_host_connections_opened_total", "counter", "Connections opened per remote host (reverse-DNS name, else IP; capped at 500 hosts).")
		hosts := make([]string, 0, len(m.hostOpened))
		for k := range m.hostOpened {
			hosts = append(hosts, k)
		}
		sort.Strings(hosts)
		for _, h := range hosts {
			fmt.Fprintf(&b, "%sremote_host_connections_opened_total%s %d\n", p, lbl("host", h), m.hostOpened[h])
		}
	}
	m.mu.Unlock()

	// poller
	if m.pollInterval != nil {
		head(&b, "poll_interval_seconds", "gauge", "Current poll interval (changeable from the UI).")
		fmt.Fprintf(&b, "%spoll_interval_seconds %s\n", p, ff(m.pollInterval().Seconds()))
	}
	if m.pollPaused != nil {
		v := 0
		if m.pollPaused() {
			v = 1
		}
		head(&b, "poll_paused", "gauge", "1 while polling is paused from the UI.")
		fmt.Fprintf(&b, "%spoll_paused %d\n", p, v)
	}
	head(&b, "polls_total", "counter", "nettop polls attempted.")
	fmt.Fprintf(&b, "%spolls_total %d\n", p, m.polls.Load())
	head(&b, "poll_errors_total", "counter", "nettop polls that failed.")
	fmt.Fprintf(&b, "%spoll_errors_total %d\n", p, m.pollErrors.Load())
	head(&b, "poll_duration_seconds", "summary", "Time spent per poll (nettop + DNS + GeoIP + diff).")
	fmt.Fprintf(&b, "%spoll_duration_seconds_sum %s\n", p, ff(float64(m.pollNanos.Load())/1e9))
	fmt.Fprintf(&b, "%spoll_duration_seconds_count %d\n", p, m.polls.Load())
	head(&b, "last_successful_poll_timestamp_seconds", "gauge", "Unix time of the last successful poll.")
	fmt.Fprintf(&b, "%slast_successful_poll_timestamp_seconds %s\n", p, ff(float64(m.lastPollUnix.Load())/1e9))

	// dns
	head(&b, "dns_lookups_total", "counter", "Reverse-DNS lookups performed, by result.")
	fmt.Fprintf(&b, "%sdns_lookups_total%s %d\n", p, lbl("result", "success"), m.dnsSuccess.Load())
	fmt.Fprintf(&b, "%sdns_lookups_total%s %d\n", p, lbl("result", "failure"), m.dnsFailure.Load())
	fmt.Fprintf(&b, "%sdns_lookups_total%s %d\n", p, lbl("result", "timeout"), m.dnsTimeout.Load())
	if m.dnsCache != nil {
		hits, misses := m.dnsCache.Stats()
		head(&b, "dns_cache_entries", "gauge", "Entries in the reverse-DNS cache (including negative results).")
		fmt.Fprintf(&b, "%sdns_cache_entries %d\n", p, m.dnsCache.Len())
		head(&b, "dns_cache_requests_total", "counter", "Reverse-DNS cache lookups, by result.")
		fmt.Fprintf(&b, "%sdns_cache_requests_total%s %d\n", p, lbl("result", "hit"), hits)
		fmt.Fprintf(&b, "%sdns_cache_requests_total%s %d\n", p, lbl("result", "miss"), misses)
	}
	head(&b, "cache_saves_total", "counter", "Cache snapshots written to disk.")
	fmt.Fprintf(&b, "%scache_saves_total %d\n", p, m.cacheSaves.Load())
	head(&b, "cache_save_errors_total", "counter", "Cache snapshots that failed to write.")
	fmt.Fprintf(&b, "%scache_save_errors_total %d\n", p, m.cacheSaveErrors.Load())

	if g := m.geo; g.Enabled() {
		head(&b, "geoip_lookups_total", "counter", "GeoIP database lookups for public addresses.")
		fmt.Fprintf(&b, "%sgeoip_lookups_total %d\n", p, g.lookups.Load())
		head(&b, "geoip_lookups_found_total", "counter", "GeoIP lookups that returned data.")
		fmt.Fprintf(&b, "%sgeoip_lookups_found_total %d\n", p, g.found.Load())
		head(&b, "geoip_lookup_errors_total", "counter", "GeoIP lookups that failed to decode.")
		fmt.Fprintf(&b, "%sgeoip_lookup_errors_total %d\n", p, g.errors.Load())
		head(&b, "geoip_database_build_timestamp_seconds", "gauge", "Build time of each loaded GeoIP database (alert if it gets old).")
		for _, d := range g.dbs {
			fmt.Fprintf(&b, "%sgeoip_database_build_timestamp_seconds%s %d\n", p, lbl("type", d.Type, "path", d.Path), d.BuildTime.Unix())
		}
	}

	// web + log
	head(&b, "sse_clients", "gauge", "Browsers currently connected to the live event stream.")
	fmt.Fprintf(&b, "%ssse_clients %d\n", p, m.sseClients.Load())
	head(&b, "sse_clients_dropped_total", "counter", "Streaming clients dropped for being too slow.")
	fmt.Fprintf(&b, "%ssse_clients_dropped_total %d\n", p, m.sseDropped.Load())
	head(&b, "log_records_written_total", "counter", "JSON connection-log records written.")
	fmt.Fprintf(&b, "%slog_records_written_total %d\n", p, m.logWritten.Load())
	head(&b, "log_write_errors_total", "counter", "JSON connection-log write failures.")
	fmt.Fprintf(&b, "%slog_write_errors_total %d\n", p, m.logErrors.Load())

	// go runtime
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Fprintf(&b, "# TYPE go_goroutines gauge\ngo_goroutines %d\n", runtime.NumGoroutine())
	fmt.Fprintf(&b, "# TYPE go_memstats_alloc_bytes gauge\ngo_memstats_alloc_bytes %d\n", ms.Alloc)
	fmt.Fprintf(&b, "# TYPE go_memstats_sys_bytes gauge\ngo_memstats_sys_bytes %d\n", ms.Sys)

	_, _ = w.Write(b.Bytes())
}
