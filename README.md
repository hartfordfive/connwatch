# connwatch

Watches every IP connection on your Mac and shows it live in a browser.

For each connection you get the **process** (name and pid), **source** (local ip:port) and
**destination** (ip:port plus its reverse-DNS name). You also get its **GeoIP location**
(country and city) and **network** (ASN and organization), **bytes received and sent** with
the live transfer rate, protocol (tcp, udp, quic), TCP state, direction, network interface
and age.

- Live web UI at `/` (Server-Sent Events, no external assets)
- Prometheus metrics at `/metrics`
- Optional JSON-lines connection log (`-log-file`)
- App logs are JSON on stderr
- Reverse-DNS results are cached on disk, so restarts don't repeat lookups
- GeoIP uses local database files, so no IP addresses are sent anywhere

## Quick start

    make geoip     # download the free DB-IP Lite City + ASN databases into ./data (~130 MB)
    make run       # build and run, then open http://127.0.0.1:9100

No `sudo` is needed: `nettop` reports every process's connections to a normal user.
If port 9100 is taken (node_exporter uses it), add `ARGS="-listen 127.0.0.1:9110"`.

Run `make` with no target to list all targets:

| Target | What it does |
|---|---|
| `make version` | print the version from `version.txt` |
| `make build` | build `./connwatch` for this machine |
| `make build-all` | macOS arm64, amd64 and universal binaries in `dist/` |
| `make test` / `make vet` / `make check` | tests (with `-race`), gofmt + vet, both |
| `make run ARGS="..."` | build, then `./connwatch -log-file ./connections.jsonl ARGS` |
| `make run-sudo` | same, as root |
| `make run-headless` | no web UI, only `/metrics` and `/healthz` (see below) |
| `make geoip` | download DB-IP Lite City + ASN (no account needed) |
| `make geoip-maxmind` | download MaxMind GeoLite2 City + ASN with `geoipupdate` |
| `make clear-cache` | delete the persisted DNS cache |
| `make clean` | remove build output |

## Versioning

The version lives in **`version.txt`** (currently `0.1.0`). To release, edit that file and
rebuild. `make build` passes it to the binary with `-ldflags "-X main.version=..."`, and a plain
`go build` gets the same value because the file is also embedded at compile time. You can
override it for one build with `make build VERSION=1.2.3-rc1`.

The version is shown by `./connwatch -version`, in the startup log line, in the UI's
`/api/connections` payload and in the `connwatch_build_info{version="..."}` metric.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `-listen` | `127.0.0.1:9100` | UI / `/events` / `/metrics` address (no auth, keep it on loopback) |
| `-interval` | `2s` | poll period (250ms–5m). It can also be changed live from the UI. |
| `-headless` | off | no web UI: serve only `/metrics` and `/healthz`, poll at the fixed `-interval` |
| `-nettop` | `/usr/bin/nettop` | path to nettop |
| `-log-file` | off | append JSON lines for every open/update/close |
| `-log-level` | `info` | app log level (JSON on stderr) |
| `-dns` | `true` | reverse-resolve remote IPs |
| `-dns-ttl` | `6h` | cache lifetime for a successful reverse lookup |
| `-dns-negative-ttl` | `10m` | cache lifetime for a failed lookup (so failures get retried) |
| `-dns-timeout` / `-dns-workers` | `1s` / `16` | resolver tuning |
| `-cache-dir` | `~/Library/Caches/connwatch` | where `dns.json` is persisted; empty = memory only |
| `-cache-save-interval` | `5m` | how often the cache is written (it's also written on shutdown) |
| `-geoip-city-db` | auto | City or Country `.mmdb` |
| `-geoip-asn-db` | auto | ASN `.mmdb` |
| `-no-geoip` | off | disable GeoIP |
| `-include-listening` | off | also show listening/unconnected sockets |
| `-ignore-loopback` | off | hide connections to 127.0.0.1 / ::1 |
| `-metrics-by-host` | off | per-remote-host counter (capped at 500 hosts) |

## Changing the interval or pausing (web UI)

The header has a **Poll every** dropdown (0.5s to 1m) and a **Pause / Resume** button:

- **Interval changes** apply immediately and are shared by every open browser tab. The
  `-interval` flag is only the starting value (default 2s), and a restart goes back to it.
- **Pause** stops polling on the server, so `nettop` doesn't run at all. Connections that
  open and close while paused are never seen. Whatever was active is shown as it was, and
  on resume anything that closed in the meantime is reported closed. The status dot turns
  amber while paused, and `connwatch_poll_paused` is 1.
- **Freeze table** (next to the filters) is different: it only stops redrawing the table in
  your browser, and polling continues.

The same settings are available over HTTP:

    curl -s localhost:9100/api/settings
    curl -s -X PUT -H 'Content-Type: application/json' -d '{"interval_ms":1000}' localhost:9100/api/settings
    curl -s -X PUT -H 'Content-Type: application/json' -d '{"paused":true}'      localhost:9100/api/settings

Requests from other websites are refused: the endpoint requires a JSON body and a matching
`Origin`, so a page you visit can't change your settings in the background.

## Headless mode

    ./connwatch --headless --interval 5s -log-file /var/log/connwatch.jsonl -listen 127.0.0.1:9100

For running as a service. Only `/metrics` and `/healthz` are served. The UI, `/events`,
`/api/connections` and `/api/settings` return 404, so the interval stays fixed at
`--interval` and can't be paused. Polling, GeoIP, the DNS cache and the JSON connection log
all work as usual. (Go flags accept both `-headless` and `--headless`.)

## GeoIP

GeoIP needs two local databases in MaxMind's `.mmdb` format: City (or Country) and ASN.
Lookups take microseconds, with no network and no rate limits.

If no paths are given, connwatch looks in `./data`, then `data/` next to the binary, then
`-cache-dir`, for these files (first match wins):

- City: `GeoLite2-City.mmdb`, `dbip-city-lite.mmdb`, `GeoLite2-Country.mmdb`, `dbip-country-lite.mmdb`
- ASN: `GeoLite2-ASN.mmdb`, `dbip-asn-lite.mmdb`

You can use either source:

- **DB-IP Lite** (`make geoip`): free, no account. Licensed CC BY 4.0, so if you publish
  anything built on it, credit "IP Geolocation by DB-IP" (https://db-ip.com).
- **MaxMind GeoLite2** (`make geoip-maxmind`): free account and license key. Run
  `brew install geoipupdate`, then create `GeoIP.conf` (it's in `.gitignore`):

      AccountID 123456
      LicenseKey your_license_key
      EditionIDs GeoLite2-City GeoLite2-ASN

Both are updated monthly. Re-run the make target, or schedule it with cron or launchd, and
restart connwatch. The metric `connwatch_geoip_database_build_timestamp_seconds` lets you
alert on a stale database, and the app logs a warning at startup if one is older than 60 days.

Private, loopback, link-local and CGNAT addresses are skipped and shown as `private` in the UI.

## DNS cache

Reverse-DNS (PTR) answers are kept in a TTL cache saved as JSON at
`~/Library/Caches/connwatch/dns.json`. If you run with `sudo`, `~` is whichever home
directory sudo gives root, so pass `-cache-dir` to be explicit. The cache works like this:

- Failed lookups are cached too, for a shorter time, so a host with no PTR record isn't
  queried on every poll.
- Writes are atomic (temp file, then rename), so a crash can't corrupt the file. A corrupt
  or old-format file is ignored with a warning.
- Expired entries are dropped on load and on save. The cache is capped at 100k entries.
- `connwatch_dns_cache_requests_total{result="hit|miss"}` shows how well it's working.

## Endpoints

Endpoints marked * are not served with `--headless`.

- `GET /` web UI *
- `GET /events` SSE stream * (`snapshot`, `open`, `update`, `close`, `stats`, `settings`, and
  `traffic`: `{id: [bytes_in, bytes_out, rate_in, rate_out]}` every poll)
- `GET /api/connections` current snapshot as JSON *
- `GET /api/settings`, `PUT /api/settings` poll interval and pause state *
- `GET /metrics` Prometheus text format
- `GET /healthz`

## Connection log (JSON lines)

    {"ts":"2026-11-07T15:35:12.31Z","event":"open","pid":1102,"process":"Google Chrome Helper","user":"alain","proto":"tcp","family":"IPv4","state":"ESTABLISHED","direction":"outbound","src_ip":"192.168.68.74","src_port":57794,"dst_ip":"64.233.178.188","dst_port":5228,"dst_host":"ol-in-f188.1e100.net","dst_country":"US","dst_country_name":"United States","dst_city":"Mountain View","dst_lat":37.4225,"dst_lon":-122.085,"dst_asn":15169,"dst_org":"Google LLC","bytes_in":5120,"bytes_out":980,"interface":"en0"}
    {"ts":"2026-11-07T15:36:01.90Z","event":"close", ... ,"bytes_in":2521252,"bytes_out":519,"duration_ms":49600}

The `close` record carries the connection's final byte totals. Byte changes don't produce
log records of their own, which keeps the log to a few lines per connection.

`event` is `open`, `update` (a TCP state change, or a hostname that resolved later) or `close`.
Send `SIGHUP` to reopen the file after log rotation.

## Prometheus

    scrape_configs:
      - job_name: connwatch
        static_configs: [{targets: ["127.0.0.1:9100"]}]

| Metric | Labels |
|---|---|
| `connwatch_connections_active` | `proto`, `state`, `direction` |
| `connwatch_connections_active_by_process` | `process` (top 100) |
| `connwatch_connections_active_by_country` | `country` (ISO code) |
| `connwatch_connections_active_by_asn` | `asn`, `org` (top 100) |
| `connwatch_connections_opened_total` / `_closed_total` | `proto`, `direction` |
| `connwatch_connection_duration_seconds` (histogram) | `proto` |
| `connwatch_bytes_total` | `proto`, `direction` = in or out |
| `connwatch_bytes_by_process_total` | `process` (first 100, then `other`), `direction` |
| `connwatch_bytes_by_country_total` | `country`, `direction` |
| `connwatch_bytes_by_asn_total` | `asn`, `org` (first 100, then `other`), `direction` |
| `connwatch_dns_lookups_total` | `result` = success, failure or timeout |
| `connwatch_dns_cache_entries`, `connwatch_dns_cache_requests_total` | `result` = hit or miss |
| `connwatch_cache_saves_total`, `connwatch_cache_save_errors_total` | |
| `connwatch_geoip_lookups_total`, `_found_total`, `_errors_total` | |
| `connwatch_geoip_database_build_timestamp_seconds` | `type`, `path` |
| `connwatch_poll_interval_seconds`, `connwatch_poll_paused` | |
| `connwatch_poll_*`, `connwatch_sse_clients`, `connwatch_log_records_written_total` | |

## How it works

    nettop (macOS per-flow network statistics: sockets + byte counters)
       + ps (full process name and user, only for pids not seen before)
       -> parse -> classify direction
       -> enrich: reverse DNS (persisted TTL cache, parallel, time-boxed)
                  GeoIP + ASN (local .mmdb, in-memory cache)
       -> Tracker diffs poll N against poll N-1 -> open/update/close events
       -> sent to: SSE hub (browser) | JSON log file | Prometheus

| File | Role |
|---|---|
| `collector.go` | runs and parses `nettop -L 1 -x` CSV, and maps pids to names via `ps` |
| `tracker.go` | connection model and poll-to-poll diffing |
| `resolver.go` | reverse DNS on top of the cache |
| `cache.go` | generic TTL cache with JSON persistence |
| `geoip.go` | `.mmdb` lookups (DB-IP and MaxMind use the same layout) |
| `metrics.go` | Prometheus text exporter (no client library) |
| `connlog.go` | JSON-lines connection log |
| `hub.go` | SSE fan-out |
| `web/index.html` | the UI, embedded into the binary |

## Limitations

- **No eBPF on macOS.** connwatch polls `nettop`, so a connection that opens and
  closes between two polls is missed. Lower `-interval` to catch more.
- **Destination names come from reverse DNS (PTR)**, not the name the app looked up. CDNs
  often return generic names or nothing. The ASN/org column usually tells you more in
  those cases.
- GeoIP for anycast and CDN addresses (Cloudflare, Fastly, Google) shows where the
  network is registered, not where the server you reached actually is.
- Durations are accurate to about one poll interval.
- **Byte counts miss the tail end of a connection.** Counters are read once per poll, so
  bytes sent in the last poll interval before a socket closes aren't counted. Long-lived
  connections are accurate; short bursts are undercounted.
- Byte counts are what the kernel counts on the socket (payload, not IP/TCP headers). A
  connection that was already open when connwatch started reports everything since it opened.
- `nettop`'s output format isn't documented by Apple. If a macOS update changes it,
  `collector.go` may need adjusting (polls fail with a parse error in the app log).

## Credits

Designed and maintained by Alain Lefebvre. Much of the code was written with the
help of Claude (Anthropic), an AI assistant, and reviewed and tested by me.

IP geolocation by [DB-IP](https://db-ip.com) when using the DB-IP Lite databases
(licensed CC BY 4.0, which requires this credit wherever the data is shown), or by
MaxMind GeoLite2 when using those.

## License

Copyright 2026 Alain Lefebvre

Licensed under the [Apache License, Version 2.0](LICENSE). You may use, modify and
distribute this software freely, including commercially, as long as you keep the
copyright and [NOTICE](NOTICE) file and state any significant changes you made.
It is provided "as is", without warranty of any kind.
