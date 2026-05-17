# OpenSearch Security audit log — Vector → ECS

Production-tested [Vector](https://vector.dev) VRL transforms that parse
OpenSearch Security plugin audit events and map them to the
[Elastic Common Schema (ECS)](https://www.elastic.co/guide/en/ecs/current/index.html).

The OpenSearch Security plugin emits one JSON object per audit event using the
`audit_*` field prefix (format version 4) — either to a log4j appender or to a
custom webhook endpoint.  This pipeline parses both REST-layer authentication
events and TRANSPORT-layer index/privilege events, routes them into dedicated
remaps, and produces ECS-shaped JSON with raw fields preserved under the
`opensearch.audit.*` vendor namespace.  Point it at a file of audit events, run
Vector, and get ECS-shaped JSON on stdout — parser-coverage gaps surface as
tagged `pipeline_error` events on stderr rather than being silently dropped.

## Quickstart

1. **Install Vector.**  See [vector.dev/docs/setup](https://vector.dev/docs/setup/installation/).

2. **Put your audit log in NDJSON form.**  OpenSearch writes one JSON object per
   line when you configure the Security plugin with a file appender or a webhook
   that serialises to NDJSON.  Copy or symlink the file to `samples.ndjson` next
   to `vector.yaml` (or update `sources.audit_log.include` to point at the real
   path on your host).

3. **Run Vector.**
   ```sh
   vector --config vector.yaml
   ```
   ECS-mapped events stream to **stdout**; parse errors and unrecognised shapes
   appear on **stderr** as `pipeline_error` events.

4. **Wire in your real sink.**  Replace the `stdout_out` console sink with your
   actual destination (Kafka, Elasticsearch, OpenSearch, …).  The events are
   ECS-shaped JSON; most sinks work without further transforms.

Two console sinks are defined in the example:

| Sink | Stream | Inputs |
|---|---|---|
| `stdout_out` | stdout | parsed events from `remap_rest` and `remap_transport` |
| `stderr_errors` | stderr | non-noise aborts and `_unmatched`-route events as ECS `pipeline_error` |

For production, swap `stdout_out` for your real sink and route `stderr_errors`
to a separate destination so coverage gaps are never lost in the main event
stream.

## What it parses

The pipeline routes audit events by `audit_request_layer` into two partitions:

| Partition | Discriminator | Audit categories |
|---|---|---|
| `rest` | `.audit_request_layer == "REST"` | `AUTHENTICATED` (outcome: success), `FAILED_LOGIN` (outcome: failure) |
| `transport` | `.audit_request_layer == "TRANSPORT"` | `INDEX_EVENT`, `GRANTED_PRIVILEGES`, `MISSING_PRIVILEGES`, `OPENSEARCH_SECURITY_INDEX_ATTEMPT` |

Compliance categories (`COMPLIANCE_DOC_READ`, `COMPLIANCE_DOC_WRITE`) are **not
currently mapped** — they were not enabled in the capture corpus.  If you need
them, extend `route_partitions` and add a `remap_compliance` transform following
the same build-out-then-assign pattern as `remap_rest`.

Anything that matches neither partition flows through `route_partitions._unmatched`
and is preserved as a `pipeline_error` event on stderr.

## ECS mapping highlights

| Source field | ECS / namespaced path | Notes |
|---|---|---|
| `@timestamp` | `@timestamp` | Re-parsed from ISO-8601; millisecond precision |
| `audit_request_effective_user` | `user.name` | Lowercased; absent on `FAILED_LOGIN` by design |
| `audit_request_effective_user_is_admin = true` | `user.roles = ["admin"]` | Only emitted when `true` |
| `audit_cluster_name` | `observer.name` | The OpenSearch cluster name |
| `audit_node_name` | `observer.hostname` | |
| `audit_node_host_address` | `observer.ip[]` | |
| `audit_rest_request_method` | `http.request.method` | REST partition only |
| `audit_rest_request_path` | `url.path` | REST partition only |
| `audit_request_privilege` | `opensearch.audit.request.privilege` | TRANSPORT only |
| `audit_trace_resolved_indices` | `opensearch.audit.trace.resolved_indices` | Arrays kept as-is |
| `audit_request_remote_address` | `opensearch.audit.request.remote_address` **only** | See "Known gotchas" |
| (all `audit_*` fields) | `opensearch.audit.*` | Full vendor namespace for SIEM drill-down |
| (synthesized) | `event.kind = "event"`, `event.module = "opensearch"`, `event.dataset = "opensearch.audit"` | |

ECS `event.category` and `event.type` are derived from `audit_category`:

| `audit_category` | `event.category` | `event.type` | `event.outcome` |
|---|---|---|---|
| `AUTHENTICATED` | `["authentication"]` | `["start"]` | `success` |
| `FAILED_LOGIN` | `["authentication"]` | `["start"]` | `failure` |
| `INDEX_EVENT` | `["database"]` | `["change"]` | `success` |
| `GRANTED_PRIVILEGES` | `["iam", "network"]` | `["allowed"]` | `success` |
| `MISSING_PRIVILEGES` | `["iam", "network"]` | `["denied"]` | `failure` |
| `OPENSEARCH_SECURITY_INDEX_ATTEMPT` | `["iam", "database"]` | `["change"]` | `unknown` |

## Known gotchas

- **`audit_request_remote_address` is the OpenSearch node, not the client.**
  OpenSearch Security records the intra-cluster peer address (typically an
  RFC1918 pod or node IP), not the originating client.  This field is kept only
  under `opensearch.audit.request.remote_address`; it is intentionally **not**
  promoted to `source.ip` because it would mislead GeoIP enrichment and
  network-origin detection rules.

- **`user.name` may be a hash or an X.509 DN.**  OpenSearch Security represents
  certificate-based identities as X.509 Distinguished Names (`cn=…`) and some
  internal identities as SHA-256 hashes (40+ hex characters).  The production
  enrichment stub in the VRL comments shows how to skip the userdb lookup for
  these opaque forms.  Downstream detection rules should not assume `user.name`
  is always a human-readable string.

- **`audit_trace_resolved_indices` arrays can be very large.**  A single bulk
  operation touching many indices produces one audit event whose
  `audit_trace_resolved_indices` array can contain hundreds or thousands of
  entries.  The pipeline preserves the array as-is under
  `opensearch.audit.trace.resolved_indices`.  If your index mapping or
  downstream storage cannot handle large arrays, truncate or sample the array
  in an extra remap step before the sink.

- **`pipeline_error` events surface on stderr (not silently dropped).**  Every
  remap runs with `drop_on_abort: true` and `reroute_dropped: true`.  The
  `silent_drop_audit` transform converts aborts and unmatched-route events into
  ECS-conformant `pipeline_error` events (tagged `_pipeline_error`) and emits
  them on `stderr_errors`.  Parser-coverage gaps are always visible — monitor
  `stderr_errors` in production so new OpenSearch Security audit shapes don't
  disappear unnoticed.

## Enrichment (optional)

This example pipeline is a pure parser / ECS normaliser.  Two enrichment paths
are common in production deployments; you can bolt either on without changing
the per-partition remap bodies.

### Username → canonical identity

Replace `user.name` with a canonical UPN and optionally add `user.department`
from your AD / IdP source.  The common Vector idiom:

1. Declare a `memory`
   [enrichment table](https://vector.dev/docs/reference/configuration/global-options/#enrichment_tables.type.memory)
   keyed by `user:<lowercased-sam-or-upn>` (value: canonical UPN) and
   `department:<canonical-upn>` (value: department name).
2. Feed it from your IdP — e.g. an
   [`exec`](https://vector.dev/docs/reference/configuration/sources/exec/)
   source on pod start plus an
   [`http_client`](https://vector.dev/docs/reference/configuration/sources/http_client/)
   periodic refresh, piped through a `remap` that pivots `{key, value}` rows
   into the table.
3. In each `remap_*` partition, after the `user.name` assignment, uncomment and
   complete the `# ── userdb enrichment (optional) ──` stub block already
   present in `vector.yaml`.

### GeoIP

`audit_request_remote_address` is typically an RFC1918 pod IP so GeoIP
enrichment is off by default.  If your deployment routes audit events through an
external address (e.g. the OpenSearch ingress endpoint rather than the pod IP),
enable GeoIP enrichment:

1. Declare `geoip` enrichment tables for the MaxMind City and ASN databases:
   ```yaml
   enrichment_tables:
     geoip_city:
       type: geoip
       path: /var/lib/GeoIP/GeoLite2-City.mmdb
       locale: en
     geoip_asn:
       type: geoip
       path: /var/lib/GeoIP/GeoLite2-ASN.mmdb
   ```
2. Promote `opensearch.audit.request.remote_address` to `source.ip` in both
   remap partitions, gate on `is_ipv4(source.ip)` and a private-CIDR skip list,
   then call `get_enrichment_table_record("geoip_city", {"ip": .source.ip})` to
   populate `source.geo.*` and `source.as.*`.

## Authorized noise drops

No audit payload classes are currently authorized as noise drops for this
source.  Every event — including unrecognised shapes — surfaces as a
`pipeline_error` on stderr rather than being silently discarded.

If you need to silence a known-irrelevant payload class, add an
`abort "noise:<reason>"` in the matching remap and document the rationale here.

## Error contract

Every remap (`parse_audit_line`, `remap_rest`, `remap_transport`) runs with
`drop_on_abort: true` and `reroute_dropped: true`.  The `silent_drop_audit`
transform consumes each remap's `.dropped` output.  It silences only aborts
whose message starts with `noise:` (none currently); everything else, plus the
`route_partitions._unmatched` lane, is converted into an ECS-conformant
`pipeline_error` event with:

- `event.kind = "pipeline_error"`
- `event.category = ["process"]`
- `event.type = ["error"]`
- `event.outcome = "failure"`
- `error.type` and `error.message` populated
- `event.original` preserved so the raw audit JSON can be replayed
- `.tags = ["_pipeline_error"]`

…and emitted on the `stderr_errors` sink.

## Customization

- **Different source shape.**  If your OpenSearch cluster POSTs audit events
  directly to a Vector endpoint (e.g. via the `webhook.http_enabled` Security
  plugin setting), replace the `audit_log` `file` source with `http_server` and
  remove `parse_audit_line` — the HTTP source decodes the JSON for you:
  ```yaml
  sources:
    audit_log:
      type: http_server
      address: "0.0.0.0:8080"
      decoding:
        codec: json
  ```
  Point `route_partitions.inputs` at the source directly.

- **Different sink.**  Replace `stdout_out` with your real sink.  Events are
  ECS-shaped JSON; most sinks work without further transforms.

- **Split partitions to different sinks.**  `remap_rest` and `remap_transport`
  can feed separate sinks — useful when REST auth events and TRANSPORT privilege
  events have different retention requirements.

## References

- [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html)
- [OpenSearch Security — Audit logs field reference](https://docs.opensearch.org/latest/security/audit-logs/field-reference/)
- [OpenSearch Security — Configuring audit logs](https://docs.opensearch.org/latest/security/audit-logs/configuring/)
- [Vector — file source](https://vector.dev/docs/reference/configuration/sources/file/)
- [Vector — http_server source](https://vector.dev/docs/reference/configuration/sources/http_server/)
- [Vector — VRL reference](https://vector.dev/docs/reference/vrl/)
- [Vector — enrichment tables (geoip / memory)](https://vector.dev/docs/reference/configuration/global-options/#enrichment_tables)
