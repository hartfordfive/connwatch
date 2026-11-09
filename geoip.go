// Copyright 2026 Alain Lefebvre
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
)

// GeoInfo is the GeoIP enrichment attached to a remote address.
type GeoInfo struct {
	CountryCode string  `json:"cc,omitempty"`
	Country     string  `json:"country,omitempty"`
	City        string  `json:"city,omitempty"`
	Lat         float64 `json:"lat,omitempty"`
	Lon         float64 `json:"lon,omitempty"`
	ASN         uint    `json:"asn,omitempty"`
	Org         string  `json:"org,omitempty"`
}

func (g GeoInfo) Empty() bool { return g == GeoInfo{} }

// The field layout shared by MaxMind GeoLite2/GeoIP2 and DB-IP Lite databases.
type mmdbCity struct {
	Country struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"country"`
	RegisteredCountry struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"registered_country"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Location struct {
		Latitude  float64 `maxminddb:"latitude"`
		Longitude float64 `maxminddb:"longitude"`
	} `maxminddb:"location"`
}

type mmdbASN struct {
	Number uint   `maxminddb:"autonomous_system_number"`
	Org    string `maxminddb:"autonomous_system_organization"`
}

type dbInfo struct {
	Path      string
	Type      string
	BuildTime time.Time
}

// GeoIP looks addresses up in local .mmdb databases (city and/or ASN).
// Lookups are local and take microseconds; results are kept in a small
// in-memory TTL cache only to avoid re-decoding on every poll.
type GeoIP struct {
	city, asn *maxminddb.Reader
	dbs       []dbInfo
	cache     *TTLCache[GeoInfo]

	lookups atomic.Uint64
	found   atomic.Uint64
	errors  atomic.Uint64
}

func OpenGeoIP(cityPath, asnPath string) (*GeoIP, error) {
	g := &GeoIP{cache: NewTTLCache[GeoInfo]("geoip", "", 50000)}
	open := func(p string) (*maxminddb.Reader, error) {
		r, err := maxminddb.Open(p)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", p, err)
		}
		g.dbs = append(g.dbs, dbInfo{Path: p, Type: r.Metadata.DatabaseType, BuildTime: r.Metadata.BuildTime()})
		return r, nil
	}
	var err error
	if cityPath != "" {
		if g.city, err = open(cityPath); err != nil {
			return nil, err
		}
	}
	if asnPath != "" {
		if g.asn, err = open(asnPath); err != nil {
			g.Close()
			return nil, err
		}
	}
	return g, nil
}

func (g *GeoIP) Enabled() bool { return g != nil && (g.city != nil || g.asn != nil) }

func (g *GeoIP) Close() {
	if g == nil {
		return
	}
	if g.city != nil {
		_ = g.city.Close()
	}
	if g.asn != nil {
		_ = g.asn.Close()
	}
}

// Lookup returns GeoIP data for a public address; private, loopback and
// link-local addresses return an empty GeoInfo.
func (g *GeoIP) Lookup(ip string) GeoInfo {
	if !g.Enabled() || ip == "" {
		return GeoInfo{}
	}
	if v, ok := g.cache.Get(ip); ok {
		return v
	}
	addr, err := netip.ParseAddr(ip)
	if err != nil || !isPublic(addr) {
		g.cache.Set(ip, GeoInfo{}, time.Hour)
		return GeoInfo{}
	}
	addr = addr.Unmap()
	g.lookups.Add(1)

	var info GeoInfo
	if g.city != nil {
		var rec mmdbCity
		if err := g.city.Lookup(addr).Decode(&rec); err != nil {
			g.errors.Add(1)
		} else {
			info.CountryCode, info.Country = rec.Country.ISOCode, rec.Country.Names["en"]
			if info.CountryCode == "" { // anycast / satellite ranges often only have the registered country
				info.CountryCode, info.Country = rec.RegisteredCountry.ISOCode, rec.RegisteredCountry.Names["en"]
			}
			info.City = rec.City.Names["en"]
			info.Lat, info.Lon = rec.Location.Latitude, rec.Location.Longitude
		}
	}
	if g.asn != nil {
		var rec mmdbASN
		if err := g.asn.Lookup(addr).Decode(&rec); err != nil {
			g.errors.Add(1)
		} else {
			info.ASN, info.Org = rec.Number, rec.Org
		}
	}
	if !info.Empty() {
		g.found.Add(1)
	}
	g.cache.Set(ip, info, time.Hour)
	return info
}

func isPublic(a netip.Addr) bool {
	a = a.Unmap()
	return a.IsGlobalUnicast() && !a.IsPrivate() && !a.IsLoopback() && !a.IsLinkLocalUnicast() &&
		!netip.MustParsePrefix("100.64.0.0/10").Contains(a) // carrier-grade NAT
}

// findGeoDB returns the first existing file among names in dirs.
func findGeoDB(dirs []string, names ...string) string {
	for _, d := range dirs {
		if d == "" {
			continue
		}
		for _, n := range names {
			p := filepath.Join(d, n)
			if st, err := os.Stat(p); err == nil && !st.IsDir() {
				return p
			}
		}
	}
	return ""
}
