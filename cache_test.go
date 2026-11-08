// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTTLCacheExpiryAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "dns.json")
	c := NewTTLCache[string]("dns", path, 0)
	c.Set("1.1.1.1", "one.one.one.one", time.Hour)
	c.Set("9.9.9.9", "", time.Hour) // negative result
	c.Set("8.8.8.8", "dns.google", -time.Second)

	if v, ok := c.Get("1.1.1.1"); !ok || v != "one.one.one.one" {
		t.Fatalf("get: %q %v", v, ok)
	}
	if _, ok := c.Get("8.8.8.8"); ok {
		t.Fatal("expired entry returned")
	}
	if h, m := c.Stats(); h != 1 || m != 1 {
		t.Fatalf("stats: %d hits %d misses", h, m)
	}
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}

	c2 := NewTTLCache[string]("dns", path, 0)
	n, err := c2.Load()
	if err != nil || n != 2 {
		t.Fatalf("load: n=%d err=%v (expired entry must not be persisted)", n, err)
	}
	if v, ok := c2.Get("9.9.9.9"); !ok || v != "" {
		t.Fatalf("negative entry lost: %q %v", v, ok)
	}

	// Save with no changes is a no-op; a corrupt file is reported, not fatal.
	if err := c2.Save(); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(path, []byte("{nope"), 0o644)
	if _, err := NewTTLCache[string]("dns", path, 0).Load(); err == nil {
		t.Fatal("expected error for corrupt file")
	}
	if n, err := NewTTLCache[string]("dns", filepath.Join(t.TempDir(), "missing.json"), 0).Load(); n != 0 || err != nil {
		t.Fatalf("missing file: %d %v", n, err)
	}
}

func TestTTLCacheMaxSize(t *testing.T) {
	c := NewTTLCache[int]("x", "", 10)
	for i := 0; i < 50; i++ {
		c.Set(string(rune('a'+i)), i, time.Duration(i+1)*time.Minute)
	}
	if c.Len() > 10 {
		t.Fatalf("cache grew past max: %d", c.Len())
	}
	if _, ok := c.Get(string(rune('a' + 49))); !ok {
		t.Fatal("newest entry evicted")
	}
}

func TestIsPublic(t *testing.T) {
	for ip, want := range map[string]bool{
		"8.8.8.8": true, "2606:4700::1111": true, "::ffff:1.1.1.1": true,
		"10.0.0.1": false, "192.168.1.5": false, "127.0.0.1": false, "fe80::1": false,
		"100.100.1.1": false, "::1": false, "fd00::1": false,
	} {
		if got := isPublic(netip.MustParseAddr(ip)); got != want {
			t.Errorf("isPublic(%s) = %v, want %v", ip, got, want)
		}
	}
}
