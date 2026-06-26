# SFTPGo — Vector → ECS

A self-contained [Vector](https://vector.dev) pipeline that maps
[SFTPGo](https://github.com/drakkan/sftpgo) authentication log events to the
[Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html)
(ECS 8.17), with MaxMind GeoIP enrichment on the source IP.

## Quickstart

```bash
# 1. Configure SFTPGo to POST its JSON log events to this host.
#    sftpgo.json → "http_log": { "url": "http://<this-host>:80/" }
#    (or front Vector with your own collector that forwards the JSON events)
# 2. Provide the MaxMind GeoLite2 databases (see "GeoIP" below).
# 3. run Vector
vector --config vector.yaml
```

Mapped events are printed as JSON to **stdout**; pipeline errors are printed to
**stderr**. For production, replace the `stdout_out` console sink with your real
destination (Kafka, Elasticsearch, OpenSearch, …) — it consumes exactly the
three remap outputs.

## What's covered

SFTPGo tags every log event with a `sender` field. This pipeline routes on
`sender` into three **authentication** partitions and maps each to ECS. All
other senders (file transfers, file operations, protocol internals) are
out-of-scope non-auth events and are dropped at the route
(`reroute_unmatched: false`).

| Partition (`sender`) | ECS `event.category` | ECS `event.outcome` |
|---|---|---|
| `login` | `[authentication, session]` | `success` |
| `connection_failed` | `[authentication]` | `failure`, or `unknown` for `no_auth_tried` |
| `dataprovider_postgresql` (level=warn) / `sftpd` | `[authentication]` | `failure` |

The `app_auth_failure` partition covers two application-layer failure shapes:

- **`dataprovider_postgresql`** (level=`warn`) — DB-layer auth errors. The
  username is extracted from the unstructured message
  (`error authenticating user "<username>"`).
- **`sftpd`** — SSH daemon connection failures. The source IP is extracted from
  the message (`failed to accept an incoming connection from ip "<ip>"`) and
  GeoIP-enriched.

## ECS field mapping

### `login` partition

| SFTPGo field | ECS field |
|---|---|
| `timestamp` | `@timestamp` |
| `username` | `user.name`, `related.user[]` |
| `ip` | `source.ip`, `related.ip[]` (+ `source.geo.*`, `source.as.*`) |
| `protocol` | `network.application` (lowercased) |
| `client` | `user_agent.original` (skipped when empty / `Unknown`) |
| `level` | `log.level` |
| — | `event.action = "login"`, `event.outcome = "success"` |
| `sender`, `method`, `encrypted`, `connection_id` | `sftpgo.*` |

### `connection_failed` partition

| SFTPGo field | ECS field |
|---|---|
| `timestamp` | `@timestamp` |
| `client_ip` | `source.ip`, `related.ip[]` (+ `source.geo.*`, `source.as.*`) |
| `username` | `user.name`, `related.user[]` (only when non-empty) |
| `protocol` | `network.application` |
| `error` | `error.message` |
| `login_type` | `sftpgo.login_type` (drives `event.outcome`) |

### `app_auth_failure` partition

| SFTPGo field | ECS field |
|---|---|
| `timestamp` | `@timestamp` |
| `message` | `error.message` |
| extracted username (postgres) | `user.name`, `related.user[]` |
| extracted IP (sftpd) | `source.ip`, `related.ip[]` (+ geo / as) |
| `sender` | `event.action`, `sftpgo.sender` |

Every event also carries `ecs.version = "8.17.0"`, `service.type = "sftpgo"`,
`event.module = "sftpgo"`, `event.kind = "event"`, and `event.original` (the raw
JSON event).

## GeoIP

`source.ip` is enriched with MaxMind GeoLite2 **City** and **ASN** data. The
config declares two `geoip` enrichment tables pointing at
`/usr/share/GeoIP/GeoLite2-City.mmdb` and `GeoLite2-ASN.mmdb`. Download those
free databases from [MaxMind](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)
and place them at those paths (or edit the paths in `vector.yaml`).

A skip-CIDR gate avoids pointless lookups on **RFC1918**, **loopback**, and
**link-local** ranges. Add your own internal public ranges to each
`geo_skip_cidrs` list if your deployment routes internal traffic over public IP
space.

## Authorized noise drops

Per the auth-only scope, non-auth senders are dropped at the route. These carry
no authentication signal:

- **Protocol internals:** `FTP`, `SFTP`, `ftpserverlib`, `common`
- **File transfers / operations:** `Upload`, `Download`, `Remove`, `Rename`,
  `Chmod`, `Chtimes`, `Truncate`

> ⚠️ File-transfer and file-operation events (`Upload`, `Download`, `Remove`,
> `Truncate`, `Rename`, `Chmod`) carry real SIEM signal for data-loss
> prevention and forensics. They are excluded here only because this pipeline
> is scoped to authentication. Onboard them as a separate pipeline if you need
> file-activity monitoring.

## Error contract

Errors are **never silently dropped**. Every remap runs with
`drop_on_abort: true` + `reroute_dropped: true`; the `silent_drop_audit`
transform consumes each remap's `.dropped` stream:

- Aborts prefixed `noise:` are discarded silently (authorized noise).
- Any other VRL failure becomes an ECS `pipeline_error` event
  (`event.kind = "pipeline_error"`, `error.type`, `error.message`,
  `event.original`) emitted to **stderr** so it surfaces in your logs.

## Design decisions / why

- **Build-out-then-assign-root** — each remap builds a local `out` object and
  assigns `. = out` at the end, avoiding VRL type-inference errors on nested
  path assignments.
- **`source.ip` only when present** — `connection_failed` and `app_auth_failure`
  may lack an IP (connection dropped pre-auth, or message didn't match the
  extraction regex); those events still flow, just without `source.*`.
- **`event.outcome = "unknown"`** for `no_auth_tried` — a dropped connection
  before any credential exchange is neither success nor failure.

## References

- SFTPGo: <https://github.com/drakkan/sftpgo>
- ECS: <https://www.elastic.co/guide/en/ecs/current/index.html>
- MaxMind GeoLite2: <https://dev.maxmind.com/geoip/geolite2-free-geolocation-data>
