// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// version comes from version.txt. `make build` passes it with -ldflags
// "-X main.version=..."; a plain `go build` falls back to the embedded copy.
var version = ""

//go:embed version.txt
var versionFile string

func init() {
	if version == "" {
		version = strings.TrimSpace(versionFile)
	}
	if version == "" {
		version = "dev"
	}
}

//go:embed web/index.html
var indexHTML []byte

// Source produces the current set of sockets. The default implementation polls
// nettop; macOS has no eBPF, so a different backend (e.g. a Network Extension
// helper) can be plugged in here without touching the rest of the app.
type Source interface {
	Collect(ctx context.Context) ([]rawConn, error)
}

type config struct {
	listen           string
	interval         time.Duration
	logFile          string
	logLevel         string
	nettopPath       string
	dns              bool
	dnsTimeout       time.Duration
	dnsTTL           time.Duration
	dnsNegTTL        time.Duration
	cacheDir         string
	cacheSave        time.Duration
	geoCityDB        string
	geoASNDB         string
	noGeoIP          bool
	dnsWorkers       int
	includeListening bool
	ignoreLoopback   bool
	metricsByHost    bool
	showVersion      bool
	headless         bool
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.listen, "listen", "127.0.0.1:9100", "address for the web UI, /events and /metrics")
	flag.DurationVar(&c.interval, "interval", 2*time.Second, "how often to poll the socket table (shorter = fewer missed short-lived connections)")
	flag.StringVar(&c.logFile, "log-file", "", "if set, append every connection open/update/close event to this file as JSON lines")
	flag.StringVar(&c.logLevel, "log-level", "info", "application log level: debug, info, warn, error (application logs are always JSON on stderr)")
	flag.StringVar(&c.nettopPath, "nettop", "", "path to nettop (default: search PATH, then /usr/bin/nettop)")
	flag.BoolVar(&c.dns, "dns", true, "reverse-resolve remote addresses")
	flag.DurationVar(&c.dnsTimeout, "dns-timeout", time.Second, "timeout per reverse-DNS lookup")
	flag.DurationVar(&c.dnsTTL, "dns-ttl", 6*time.Hour, "how long to cache successful reverse-DNS results")
	flag.DurationVar(&c.dnsNegTTL, "dns-negative-ttl", 10*time.Minute, "how long to cache failed reverse-DNS lookups")
	flag.StringVar(&c.cacheDir, "cache-dir", defaultCacheDir(), "directory for the persisted DNS cache (empty = don't persist)")
	flag.DurationVar(&c.cacheSave, "cache-save-interval", 5*time.Minute, "how often to write the cache to disk (also written on shutdown)")
	flag.StringVar(&c.geoCityDB, "geoip-city-db", "", "path to a City/Country .mmdb (DB-IP Lite or MaxMind GeoLite2); default: auto-detect in ./data and -cache-dir")
	flag.StringVar(&c.geoASNDB, "geoip-asn-db", "", "path to an ASN .mmdb (DB-IP Lite or MaxMind GeoLite2); default: auto-detect in ./data and -cache-dir")
	flag.BoolVar(&c.noGeoIP, "no-geoip", false, "disable GeoIP enrichment")
	flag.IntVar(&c.dnsWorkers, "dns-workers", 16, "parallel reverse-DNS lookups")
	flag.BoolVar(&c.includeListening, "include-listening", false, "also track listening / unconnected sockets")
	flag.BoolVar(&c.ignoreLoopback, "ignore-loopback", false, "hide connections whose remote end is loopback")
	flag.BoolVar(&c.metricsByHost, "metrics-by-host", false, "export a per-remote-host counter (higher cardinality, capped at 500 hosts)")
	flag.BoolVar(&c.showVersion, "version", false, "print version and exit")
	flag.BoolVar(&c.headless, "headless", false, "no web UI: serve only /metrics and /healthz, and poll at the fixed -interval (for running as a service)")
	flag.Parse()
	return c
}

func defaultCacheDir() string {
	d, err := os.UserCacheDir() // ~/Library/Caches on macOS
	if err != nil {
		return ""
	}
	return filepath.Join(d, "connwatch")
}

func newLogger(level string) *slog.Logger {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lv}))
}

func main() {
	cfg := parseFlags()
	if cfg.showVersion {
		fmt.Println(version)
		return
	}
	logger := newLogger(cfg.logLevel)
	slog.SetDefault(logger)
	if err := run(cfg, logger); err != nil {
		logger.Error("fatal", "error", err.Error())
		os.Exit(1)
	}
}

const (
	minInterval = 250 * time.Millisecond
	maxInterval = 5 * time.Minute
)

type server struct {
	cfg     config
	tracker *Tracker
	hub     *Hub
	metrics *Metrics
	logger  *slog.Logger

	interval   atomic.Int64  // current poll interval, nanoseconds
	paused     atomic.Bool   // polling paused from the UI
	settingsCh chan struct{} // wakes the poll loop after a settings change
}

func (s *server) pollInterval() time.Duration { return time.Duration(s.interval.Load()) }

type settingsPayload struct {
	IntervalMS        int64 `json:"interval_ms"`
	Paused            bool  `json:"paused"`
	DefaultIntervalMS int64 `json:"default_interval_ms"` // the -interval flag
	MinIntervalMS     int64 `json:"min_interval_ms"`
	MaxIntervalMS     int64 `json:"max_interval_ms"`
}

func (s *server) settings() settingsPayload {
	return settingsPayload{
		IntervalMS:        s.pollInterval().Milliseconds(),
		Paused:            s.paused.Load(),
		DefaultIntervalMS: s.cfg.interval.Milliseconds(),
		MinIntervalMS:     minInterval.Milliseconds(),
		MaxIntervalMS:     maxInterval.Milliseconds(),
	}
}

// sameOrigin rejects cross-site requests: any web page you visit could
// otherwise send a request to this loopback server. Requiring a JSON body
// also forces browsers to send a CORS preflight, which we never approve.
func sameOrigin(r *http.Request) bool {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		return false
	}
	o := r.Header.Get("Origin")
	if o == "" {
		return true // non-browser client, e.g. curl
	}
	u, err := url.Parse(o)
	return err == nil && u.Host == r.Host
}

func (s *server) handlePutSettings(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		http.Error(w, "forbidden: send JSON from the connwatch UI or a non-browser client", http.StatusForbidden)
		return
	}
	var req struct {
		IntervalMS *int64 `json:"interval_ms"`
		Paused     *bool  `json:"paused"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, "bad JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	changed := false
	if req.IntervalMS != nil {
		d := time.Duration(*req.IntervalMS) * time.Millisecond
		if d < minInterval || d > maxInterval {
			http.Error(w, fmt.Sprintf("interval_ms must be between %d and %d", minInterval.Milliseconds(), maxInterval.Milliseconds()), http.StatusBadRequest)
			return
		}
		if old := time.Duration(s.interval.Swap(int64(d))); d != old {
			changed = true
			s.logger.Info("poll interval changed", "from", old.String(), "to", d.String(), "remote", r.RemoteAddr)
		}
	}
	if req.Paused != nil {
		if old := s.paused.Swap(*req.Paused); old != *req.Paused {
			changed = true
			if *req.Paused {
				s.logger.Info("polling paused", "remote", r.RemoteAddr)
			} else {
				s.logger.Info("polling resumed", "remote", r.RemoteAddr)
			}
		}
	}
	if changed {
		select { // non-blocking: one pending wake-up is enough
		case s.settingsCh <- struct{}{}:
		default:
		}
		s.hub.Publish("settings", s.settings())
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.settings())
}

type snapshotPayload struct {
	Connections []Conn `json:"connections"`
	Stats       Stats  `json:"stats"`
	IntervalMS  int64  `json:"interval_ms"`
	Paused      bool   `json:"paused"`
	Version     string `json:"version"`
}

func (s *server) snapshot() snapshotPayload {
	return snapshotPayload{
		Connections: s.tracker.Snapshot(),
		Stats:       s.tracker.Stats(time.Now()),
		IntervalMS:  s.pollInterval().Milliseconds(),
		Paused:      s.paused.Load(),
		Version:     version,
	}
}

func run(cfg config, logger *slog.Logger) error {
	if cfg.interval < minInterval || cfg.interval > maxInterval {
		return fmt.Errorf("-interval must be between %s and %s", minInterval, maxInterval)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if runtime.GOOS != "darwin" {
		logger.Warn("connwatch needs macOS (it reads connections from nettop)", "goos", runtime.GOOS)
	}
	if os.Geteuid() != 0 {
		logger.Info("not running as root: nettop still reports every process, but run with sudo if some connections seem to be missing")
	}
	if host, _, err := net.SplitHostPort(cfg.listen); err == nil {
		if ip := net.ParseIP(host); host == "" || (ip != nil && !ip.IsLoopback()) {
			if cfg.headless {
				logger.Warn("metrics are exposed on a non-loopback address without authentication", "listen", cfg.listen)
			} else {
				logger.Warn("web UI is bound to a non-loopback address and has no authentication", "listen", cfg.listen)
			}
		}
	}

	metrics := NewMetrics(version, cfg.metricsByHost)
	hub := NewHub(metrics)
	tracker := NewTracker()
	// Persisted reverse-DNS cache.
	dnsCachePath := ""
	if cfg.cacheDir != "" {
		dnsCachePath = filepath.Join(cfg.cacheDir, "dns.json")
	}
	dnsCache := NewTTLCache[string]("dns", dnsCachePath, 100000)
	if n, err := dnsCache.Load(); err != nil {
		logger.Warn("could not load DNS cache", "path", dnsCachePath, "error", err.Error())
	} else if dnsCachePath != "" {
		logger.Info("loaded DNS cache", "path", dnsCachePath, "entries", n)
	}
	metrics.dnsCache = dnsCache
	resolver := NewResolver(cfg.dns, cfg.dnsTTL, cfg.dnsNegTTL, cfg.dnsTimeout, cfg.dnsWorkers, dnsCache, metrics)

	saveCaches := func() {
		if err := dnsCache.Save(); err != nil {
			metrics.cacheSaveErrors.Add(1)
			logger.Warn("could not save DNS cache", "path", dnsCachePath, "error", err.Error())
			return
		}
		metrics.cacheSaves.Add(1)
	}
	defer saveCaches()

	// GeoIP (local .mmdb files).
	var geo *GeoIP
	if !cfg.noGeoIP {
		exe, _ := os.Executable()
		dirs := []string{"data", filepath.Join(filepath.Dir(exe), "data"), cfg.cacheDir}
		city, asn := cfg.geoCityDB, cfg.geoASNDB
		if city == "" {
			city = findGeoDB(dirs, "GeoLite2-City.mmdb", "dbip-city-lite.mmdb", "GeoLite2-Country.mmdb", "dbip-country-lite.mmdb")
		}
		if asn == "" {
			asn = findGeoDB(dirs, "GeoLite2-ASN.mmdb", "dbip-asn-lite.mmdb")
		}
		if g, err := OpenGeoIP(city, asn); err != nil {
			return fmt.Errorf("geoip: %w", err)
		} else if g.Enabled() {
			geo = g
			defer geo.Close()
			for _, d := range g.dbs {
				logger.Info("loaded GeoIP database", "type", d.Type, "path", d.Path, "built", d.BuildTime.Format(time.DateOnly))
				if time.Since(d.BuildTime) > 60*24*time.Hour {
					logger.Warn("GeoIP database is more than 60 days old; run `make geoip`", "path", d.Path)
				}
			}
		} else {
			logger.Info("GeoIP disabled: no database found (run `make geoip`, or pass -geoip-city-db / -geoip-asn-db)")
		}
	}
	metrics.geo = geo

	var src Source
	collector, err := NewCollector(cfg.nettopPath, 15*time.Second)
	if err != nil {
		return err
	}
	src = collector

	var connLog *ConnLog
	if cfg.logFile != "" {
		connLog, err = NewConnLog(cfg.logFile, metrics)
		if err != nil {
			return fmt.Errorf("open connection log: %w", err)
		}
		defer connLog.Close()
		logger.Info("writing connection log", "path", cfg.logFile)

		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		go func() {
			for range hup {
				if err := connLog.Reopen(); err != nil {
					logger.Error("reopen connection log failed", "error", err.Error())
				} else {
					logger.Info("connection log reopened")
				}
			}
		}()
	}

	srv := &server{cfg: cfg, tracker: tracker, hub: hub, metrics: metrics, logger: logger,
		settingsCh: make(chan struct{}, 1)}
	srv.interval.Store(int64(cfg.interval))
	metrics.pollInterval = srv.pollInterval
	metrics.pollPaused = srv.paused.Load

	mux := http.NewServeMux()
	if !cfg.headless {
		mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			_, _ = w.Write(indexHTML)
		})
		mux.HandleFunc("GET /events", srv.handleEvents)
		mux.HandleFunc("GET /api/connections", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(w).Encode(srv.snapshot())
		})
		mux.HandleFunc("GET /api/settings", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(w).Encode(srv.settings())
		})
		mux.HandleFunc("PUT /api/settings", srv.handlePutSettings)
	}
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		metrics.Write(w)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})

	httpSrv := &http.Server{
		Addr:              cfg.listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// no WriteTimeout: /events is a long-lived stream
	}
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return err
	}
	go func() {
		if cfg.headless {
			logger.Info("listening (headless: metrics only)", "metrics", "http://"+ln.Addr().String()+"/metrics", "interval", cfg.interval.String(), "version", version)
		} else {
			logger.Info("listening", "url", "http://"+ln.Addr().String(), "interval", cfg.interval.String(), "version", version)
		}
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server", "error", err.Error())
			stop()
		}
	}()

	opts := Options{IncludeListening: cfg.includeListening, IgnoreLoopback: cfg.ignoreLoopback}

	poll := func() {
		start := time.Now()
		raws, err := src.Collect(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logger.Error("poll failed", "error", err.Error())
				metrics.ObservePoll(time.Since(start), err)
			}
			return
		}
		conns := buildConns(raws, opts)

		ips := make([]string, 0, len(conns))
		for _, c := range conns {
			if c.RemoteAddr != "" {
				ips = append(ips, c.RemoteAddr)
			}
		}
		resolver.ResolveBatch(ctx, ips)
		for i := range conns {
			conns[i].RemoteHost = resolver.Lookup(conns[i].RemoteAddr)
			if geo != nil {
				if g := geo.Lookup(conns[i].RemoteAddr); !g.Empty() {
					conns[i].Geo = &g
				}
			}
		}

		now := time.Now()
		events, traffic := tracker.Update(now, conns)
		for _, ev := range events {
			hub.Publish(ev.Type, ev)
			metrics.ObserveEvent(ev)
			if connLog != nil {
				connLog.Write(ev)
			}
			logger.Debug("connection event", "event", ev.Type, "process", ev.Conn.Process, "pid", ev.Conn.PID,
				"dst", ev.Conn.RemoteAddr, "host", ev.Conn.RemoteHost)
		}

		// Byte counters: one compact SSE frame per poll with every connection
		// whose counters moved: id -> [bytes_in, bytes_out, rate_in, rate_out].
		// Sent every poll, even when empty: connections missing from it moved
		// no bytes, so the UI resets their rate to zero.
		tf := make(map[string][4]float64, len(traffic))
		for _, tr := range traffic {
			metrics.ObserveTraffic(tr)
			c := tr.Conn
			tf[c.ID] = [4]float64{float64(c.BytesIn), float64(c.BytesOut), c.RateIn, c.RateOut}
		}
		hub.Publish("traffic", tf)

		d := time.Since(start)
		metrics.ObservePoll(d, nil)
		metrics.SetActive(tracker.Snapshot())
		st := tracker.Stats(now)
		st.PollMS = float64(d.Microseconds()) / 1000
		hub.Publish("stats", st)
	}

	poll()
	curInterval := cfg.interval
	ticker := time.NewTicker(curInterval)
	defer ticker.Stop()
	saveEvery := cfg.cacheSave
	if saveEvery <= 0 {
		saveEvery = 5 * time.Minute
	}
	saveTicker := time.NewTicker(saveEvery)
	defer saveTicker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			if !srv.paused.Load() {
				poll()
			}
		case <-srv.settingsCh:
			if d := srv.pollInterval(); d != curInterval {
				curInterval = d
				ticker.Reset(d)
			}
			if !srv.paused.Load() {
				poll() // apply right away (also the first poll after resuming)
			}
		case <-saveTicker.C:
			saveCaches()
		}
	}

	logger.Info("shutting down")
	hub.Close()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpSrv.Shutdown(sctx)
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")

	// Subscribe first, then snapshot: anything that happens in between is
	// delivered as an event and the UI applies events idempotently.
	sub := s.hub.Subscribe()
	defer s.hub.Unsubscribe(sub)

	snap := s.snapshot()
	sort.Slice(snap.Connections, func(i, j int) bool { return snap.Connections[i].FirstSeen.Before(snap.Connections[j].FirstSeen) })
	frame, err := encodeSSE("snapshot", snap)
	if err != nil {
		return
	}
	_, _ = w.Write(frame)
	fl.Flush()
	s.logger.Debug("sse client connected", "remote", r.RemoteAddr)

	ka := time.NewTicker(15 * time.Second)
	defer ka.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-sub.ch:
			if !ok {
				return
			}
			if _, err := w.Write(msg); err != nil {
				return
			}
			fl.Flush()
		case <-ka.C:
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			fl.Flush()
		}
	}
}
