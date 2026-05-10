# Infoblox NIOS — Vector → ECS

Production-tested [Vector](https://vector.dev) VRL transforms that
parse the three Infoblox NIOS syslog channels and map them to the
[Elastic Common Schema (ECS)](https://www.elastic.co/guide/en/ecs/current/index.html).
Drop in a syslog UDP listener, point your NIOS appliance at it, and
get ECS-shaped JSON on stdout.

## Quickstart

```sh
# 1. point your Infoblox NIOS appliance at this host on UDP/514
#    (Grid Manager → Logging → External Syslog Servers)
# 2. run Vector
vector --config vector.yaml
```

The example config writes events to **stdout** so you can wire it up
incrementally. Two console sinks are defined:

| Sink | Stream | Inputs |
|---|---|---|
| `stdout_out` | stdout | parsed events from all three partitions plus `tag_unrouted` (events that didn't match any discriminator, tagged `_unrouted`) |
| `stderr_errors` | stderr | non-noise aborts converted into ECS `pipeline_error` events |

For production, replace `stdout_out` with your real sink (Kafka,
Elasticsearch, OpenSearch, …) and route `stderr_errors` to a separate
destination so coverage gaps don't get lost in your main stream.

## What's covered

The pipeline routes events into three partitions by the syslog
process name and the leading message token:

| Partition | Discriminator | Source |
|---|---|---|
| `dns_responses` | `process.name == "named"` AND message starts with `infoblox-responses:` | BIND response-logging channel |
| `dns_queries` | `process.name == "named"` AND message starts with `queries:` | BIND queries channel |
| `dhcp` | `process.name == "dhcpd"` | ISC dhcpd (lease lifecycle, DDNS, Option 82, etc.) |

Anything that hits none of those discriminators flows through
`tag_unrouted` and is preserved as a `pipeline_error` event so the
analyst can extend coverage.

### dhcpd shapes parsed

- `DHCPACK / OFFER / NAK / DECLINE / EXPIRE / RELEASE` long form
  (`on <ip> to <mac> via <iface> [relay <ip>] [lease-duration <s>] …`)
- `DHCPACK to <ip> (<mac>) via <iface>` short form
- `DHCPREQUEST for <ip> [from <mac>] [via <relay-or-iface>] [TransID <hex>] [uid <uid>] [(<state>)]`
- `DHCPDISCOVER from <mac> via <relay-or-iface> TransID <hex>`
- `Option 82: received a <kind> DHCP packet from relay-agent <ip> with a circuit-id of "<cid>", a remote-id of "<rid>" for <ip> (<mac>) lease time …`
- DDNS forward / reverse map adds, removes, and failure variants
- Lease-pool free-list housekeeping (`<ip>: removing client association (free) [uid=<uid>] hw=<mac>` — uid token optional)
- Truncated `DHCPEXPIRE on <ip> to` (relay-buffer cut; tagged `_truncated`)
- A handful of dhcpd warnings (`label length …`, `parse error …`, `bad …`, `fqdn …`)

### BIND shapes parsed

- **Queries**: `queries: client @<hex> <ip>#<port> (<qname>): view <N>: query: <qname> IN <type> <flags> (<resolver-ip>) [ECS <subnet>/<src>/<scope>]`
- **Responses**: `infoblox-responses: <ts> client <ip>#<port>: <UDP|TCP>: query: <qname> IN <type> response: <rcode> <flags> [ <answers> ]`

## ECS field mapping

A summary — see the inline VRL comments for exact provenance.

### `dns_queries` partition

| Source token | ECS path |
|---|---|
| client `<ip>#<port>` | `client.ip`, `client.port` |
| `<qname>` | `dns.question.name` |
| `IN <type>` | `dns.question.type` |
| header flags (`+E(0)`, `+T`, etc.) | `infoblox_nios.log.dns.header_flags` |
| `view <N>` | `infoblox_nios.dns.view` |
| resolver IP `(<ip>)` | `infoblox_nios.dns.resolver_ip` |
| `[ECS <subnet>/<src>/<scope>]` | `infoblox_nios.dns.edns_client_subnet` |
| (synthesized) | `event.action="dns-query"`, `event.category=["network"]`, `event.type=["info","connection","protocol"]`, `event.outcome="unknown"`, `network.protocol="dns"` |

### `dns_responses` partition

| Source token | ECS path |
|---|---|
| client `<ip>#<port>` | `client.ip`, `client.port` |
| `UDP` / `TCP` | `network.transport` |
| `<qname>` | `dns.question.name` |
| `IN <type>` | `dns.question.type` |
| `response: <rcode>` | `dns.response_code` |
| `view <N>` | `infoblox_nios.dns.view` |
| answer section | `dns.answers[].data`, `dns.resolved_ip` |
| event timestamp from payload | `@timestamp` (millisecond precision) |
| (synthesized) | `event.action="dns-response"`, outcome derived from rcode (`NOERROR`/`NXDOMAIN` → `success`, `SERVFAIL`/`REFUSED`/etc → `failure`) |

### `dhcp` partition

| Source token | ECS path |
|---|---|
| leased `<ip>` | `client.ip` |
| client `<mac>` | `client.mac` (uppercase, dashed) |
| client hostname `(<name>)` | `host.hostname`, `related.hosts[]` |
| `via <iface>` | `infoblox_nios.dhcp.interface` |
| `relay <ip>` / via relay-IP | `infoblox_nios.dhcp.relay_ip`, `related.ip[]` |
| `lease-duration <secs>` | `infoblox_nios.dhcp.lease_duration` |
| `(<state>)` (RENEW / REBIND / SELECTING / …) | `infoblox_nios.dhcp.lease_state` |
| `uid <hex>` | `infoblox_nios.dhcp.client_uid` |
| `TransID <hex>` | `infoblox_nios.dhcp.transaction_id` |
| Option 82 circuit-id / remote-id | `infoblox_nios.dhcp.option82.{circuit_id,remote_id}` |
| DDNS fields | `infoblox_nios.dhcp.ddns.*` |
| (synthesized) | `event.action="dhcp-<verb>"`, `event.category=["network"]`, `event.type=["info","connection",…]`, `network.protocol="dhcp"`, `network.transport="udp"`, `network.iana_number="17"` |

## Authorized noise drops

A short list of operationally-pure lines is silently dropped — they
carry no SIEM signal and pair with a richer event a fraction of a
second later. Drops are explicit (each via a `noise:<reason>` abort
matched by `silent_drop_audit`) so a future maintainer can see the
list and the rationale.

| Drop key | Match | Why |
|---|---|---|
| `noise:syslog-mark` | `process.name == "--"` or message `MARK --` / `-- MARK --` | syslogd liveness ping; covered by Vector / k8s metrics |
| `noise:bind-formerr-malformed` | `infoblox-responses: …<unknown due to query error>… response: FORMERR …` | DNS question section was too malformed to decode a qname/class/type — no DNS signal in the event |
| `noise:lease-state-active` | dhcpd message starts with `Lease state active. Not abandoning ` | diagnostic that always pairs with the corresponding `DHCPRELEASE`/`DHCPEXPIRE` |
| `noise:dhcpd-stats-latency` | dhcpd message starts with `Average ` and contains `dynamic DNS update latency:` | periodic dhcpd self-stats; already in your metrics |
| `noise:dhcpd-stats-timeout` | dhcpd message starts with `Dynamic DNS update timeout count ` | periodic dhcpd self-stats |
| `noise:lease-publishing-disabled` | dhcpd message equals `Lease publishing disabled` | one-shot startup notice on a stand-by appliance |
| `noise:ddns-cleanup-internal` | dhcpd message starts with `DDNS: cleaning up lease pointer ` | internal callback-pointer trace; pairs with a richer DDNS event |
| `noise:dhcpd-uid-duplicate` | dhcpd message starts with `uid lease ` and contains ` is duplicate on ` | internal binding-database reconciliation; the matching lease event carries the same IP and MAC |

To add or remove a drop, edit the matching `if … abort "noise:<reason>"`
preflight in the relevant remap and update this table. Anything not
on this list must reach a sink — if a real-but-unfamiliar payload
doesn't match a known shape, it surfaces as a `pipeline_error` so the
analyst can extend the parser. **Errors are never silently dropped —
only noise is.**

## Error contract

Every remap in this pipeline runs with `drop_on_abort: true` and
`reroute_dropped: true`. The audit transform `silent_drop_audit`
consumes each remap's `.dropped` output. It silences only aborts
whose reason starts with `noise:` (the table above); everything else
is converted into an ECS-conformant `pipeline_error` event with:

- `event.kind = "pipeline_error"`
- `event.category = ["host"]`
- `event.type = ["info","error"]`
- `event.outcome = "failure"`
- `error.type` and `error.message` populated
- `event.original` preserved so the analyst can replay the raw line

…and emitted on the `stderr_errors` sink. Unrouted events
(no discriminator matched) flow through `tag_unrouted` with
`tags=["_unrouted"]` and the same pipeline_error envelope before
landing on `stdout_out` — they're parser-coverage gaps, not aborts.

## Design decisions / why

- **No `if (exists(.x))` guards.** They look defensive but quietly
  paper over real failures. We rely on `?? null`, `string!()`,
  `parse_*!()`, and let unexpected input route to the audit so it
  surfaces instead of vanishing.
- **Prefer Vector's built-in `parse_*` over hand-rolled regex.**
  `parse_regex` for the few shapes that need it; `parse_key_value`
  for the rare key=value rendering. Custom string slicing is the
  last resort.
- **Every meaningful VRL block has a `# ── header ──` comment.**
  These survived rewrites because they carry the *why*: the shape
  being matched, the BIND/dhcpd quirk being handled, the iteration
  in which a regression was found.
- **GeoIP is omitted by default.** In the environment this transform
  was developed against, every observed source IP was RFC1918, so
  enrichment would always miss. To enable it, declare a Vector
  [`enrichment_tables`](https://vector.dev/docs/reference/configuration/global-options/#enrichment_tables)
  block pointing at a MaxMind DB and add a `get_enrichment_table_record`
  call in the relevant remap, gated on `is_ipv4`/`is_ipv6` of
  `client.ip`. The natural insertion points are at the end of each
  remap's success path, just before the silent_drop_audit fall-through.
- **The audit transform is generic.** It silences `noise:*` aborts and
  promotes every other abort to a `pipeline_error`. The same shape
  works unmodified for any vendor — only the per-partition remap
  bodies are Infoblox-specific.

## Customization

- **Different syslog transport.** Swap `mode: udp` for `mode: tcp`,
  or add a TLS block — see Vector's
  [syslog source](https://vector.dev/docs/reference/configuration/sources/syslog/).
- **Different sink.** Replace `stdout_out` with your real sink. The
  upstream events are ECS-shaped JSON, so most sinks work without
  further transforms.
- **Split DNS and DHCP.** The two BIND partitions and the dhcpd
  partition can fan into separate sinks by listing the matching
  remap in different `inputs` arrays.
- **Add a pipeline-error alarm.** Wire the `silent_drop_audit` output
  into a separate sink that pages on `event.kind == "pipeline_error"`
  to catch parser-coverage drift.

## References

- [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html)
- [Vector — syslog source](https://vector.dev/docs/reference/configuration/sources/syslog/)
- [Vector — VRL reference](https://vector.dev/docs/reference/vrl/)
- [Vector — console sink](https://vector.dev/docs/reference/configuration/sinks/console/)
- [Infoblox NIOS — Logging Configuration](https://docs.infoblox.com/space/NAG8) (Grid Manager → Grid Properties → Monitoring)
