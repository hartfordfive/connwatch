// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestServer() *server {
	m := NewMetrics("test", false)
	s := &server{cfg: config{interval: 2 * time.Second}, tracker: NewTracker(), hub: NewHub(m), metrics: m,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)), settingsCh: make(chan struct{}, 1)}
	s.interval.Store(int64(2 * time.Second))
	return s
}

func put(s *server, body string, hdr map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1:9100/api/settings", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.handlePutSettings(w, r)
	return w
}

func TestSettingsIntervalAndPause(t *testing.T) {
	s := newTestServer()
	sub := s.hub.Subscribe()

	if w := put(s, `{"interval_ms":500}`, map[string]string{"Origin": "http://127.0.0.1:9100"}); w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	if s.pollInterval() != 500*time.Millisecond {
		t.Fatalf("interval not applied: %s", s.pollInterval())
	}
	select {
	case <-s.settingsCh:
	default:
		t.Fatal("poll loop not woken")
	}
	if msg := string(<-sub.ch); !strings.Contains(msg, "event: settings") || !strings.Contains(msg, `"interval_ms":500`) {
		t.Fatalf("settings not broadcast: %q", msg)
	}

	w := put(s, `{"paused":true}`, nil)
	if w.Code != 200 || !s.paused.Load() || !strings.Contains(w.Body.String(), `"paused":true`) {
		t.Fatalf("pause failed: %d %s", w.Code, w.Body)
	}
	// both channels must not block on repeated changes
	put(s, `{"paused":false}`, nil)
	put(s, `{"interval_ms":1000}`, nil)
	if s.paused.Load() || s.pollInterval() != time.Second {
		t.Fatal("resume/interval not applied")
	}
}

func TestSettingsValidationAndCSRF(t *testing.T) {
	s := newTestServer()
	for _, body := range []string{`{"interval_ms":100}`, `{"interval_ms":600000}`, `nope`} {
		if w := put(s, body, nil); w.Code != 400 {
			t.Errorf("%s: want 400, got %d", body, w.Code)
		}
	}
	if w := put(s, `{"paused":true}`, map[string]string{"Origin": "https://evil.example"}); w.Code != 403 || s.paused.Load() {
		t.Errorf("cross-origin request must be rejected: %d", w.Code)
	}
	if w := put(s, `{"paused":true}`, map[string]string{"Content-Type": "text/plain"}); w.Code != 403 {
		t.Errorf("non-JSON request must be rejected: %d", w.Code)
	}
	if s.pollInterval() != 2*time.Second {
		t.Errorf("interval changed by a rejected request: %s", s.pollInterval())
	}
}
