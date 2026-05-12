# Kubernetes API-server audit log — Vector → ECS

Production-tested [Vector](https://vector.dev) VRL transforms that parse
Kubernetes API-server audit events
([`audit.k8s.io/v1`](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/))
and map them to the
[Elastic Common Schema (ECS)](https://www.elastic.co/guide/en/ecs/current/index.html).
Point it at your kube-apiserver audit log, run Vector, and get ECS-shaped
JSON on stdout.

## Quickstart

```sh
# 1. Make sure the kube-apiserver is writing an audit log. Most distributions
#    write it to one of:
#      /var/log/kubernetes/audit/audit.log
#      /var/log/kube-apiserver-audit.log
#    Adjust `sources.kube_audit.include` in vector.yaml to match.
# 2. run Vector
vector --config vector.yaml
```

The example config writes events to **stdout** so you can wire it up
incrementally. Two console sinks are defined:

| Sink | Stream | Inputs |
|---|---|---|
| `stdout_out` | stdout | parsed events from all five partitions |
| `stderr_errors` | stderr | non-noise aborts and `_unmatched`-route events converted into ECS `pipeline_error` events |

For production, replace `stdout_out` with your real sink (Kafka,
Elasticsearch, OpenSearch, …) and route `stderr_errors` to a separate
destination so coverage gaps don't get lost in your main stream.

## What's covered

The pipeline routes audit events into five partitions by `stage`, `level`
and `objectRef` presence — partition-specific fields are handled in
dedicated remaps rather than one branch-heavy monolith.

| Partition | Discriminator | What it carries |
|---|---|---|
| `panic` | `.stage == "Panic"` | API-server crash records (rare, carries `objectRef`) |
| `response_started` | `.stage == "ResponseStarted"` | Pre-completion events (watch / streaming endpoints) |
| `non_resource` | `.stage == "ResponseComplete" && !exists(.objectRef)` | API-discovery, `/healthz`, `/metrics`, `/version` |
| `request_response_complete` | `.stage == "ResponseComplete" && exists(.objectRef) && .level == "Request"` | Full `requestObject` / `responseObject` payloads — the highest-SIEM-signal lane (RBAC, workload spec, security context) |
| `metadata_response_complete` | `.stage == "ResponseComplete" && exists(.objectRef) && .level == "Metadata"` | The bulk of audit traffic — verb + resource ref only |

Anything that hits none of those discriminators flows through
`route_partitions._unmatched` and is preserved as a `pipeline_error` event
so you can extend coverage.

## ECS field mapping (summary)

The full provenance is documented in the inline VRL comments. A high-level
summary:

| Source path | ECS / namespaced path |
|---|---|
| `auditID` | `event.id` |
| `verb` | `event.action` |
| `level` | preserved under `kubernetes.audit.level` |
| `stage` | preserved under `kubernetes.audit.stage` |
| `user.username` | `user.name` (lowercased, `oidc:` prefix stripped) |
| `user.uid` | `user.id` |
| `userAgent` | `user_agent.original` |
| `sourceIPs[0]` | `source.ip`, `client.ip` |
| `sourceIPs[*]` | `related.ip[]` |
| `objectRef.{namespace,name,resource,apiGroup,apiVersion}` | `orchestrator.namespace`, `orchestrator.resource.{name,type}` |
| `responseStatus.code` | `http.response.status_code` |
| `responseStatus.{reason,message}` | `kubernetes.audit.responseStatus.*` |
| `requestReceivedTimestamp` | `@timestamp` |
| `annotations.authorization.k8s.io/decision` | drives `event.outcome` (`allow`→`success`, `forbid`→`failure`) |
| (synthesized) | `event.kind="event"`, `event.category=["host"]`, `event.type=["info"]`, `event.module="kubernetes"`, `event.dataset="kubernetes.audit"` |

The complete raw audit payload is preserved under
`kubernetes.audit.*` (matching the Elastic ingest-pipeline shape), plus `event.original` as a
JSON-encoded copy of the original audit fields for forensic replay.

### `requestObject` / `responseObject` ingestion-safety normalisation

The high-signal partition (`request_response_complete`) carries arbitrary
Kubernetes object specs — Pods, RBAC bindings, Services, etc. — under
`kubernetes.audit.requestObject` and `responseObject`. Two patterns in those
trees break OpenSearch / Elasticsearch dynamic mapping:

1. **Dotted keys.** Kubernetes labels and selectors use keys like
   `app.kubernetes.io/name`. The `.` is read as a path separator and
   collides with prior nested-path mappings.
2. **`IntOrString` fields.** `Service.spec.ports[].targetPort` (and a
   handful of others) can be int or string per object; first-write-wins on
   field type causes the second-type write to fail.

The pipeline normalises both in-place rather than dropping the subtree:

- A recursive `map_keys(recursive: true)` pass rewrites every `.` → `_` in
  any nested key. The full subtree, including `containers[*].securityContext.*`
  and selectors, survives end-to-end.
- A short `for_each` over `spec.ports[]` coerces `targetPort` to string.

`.metadata` is still deleted (it carries `managedFields` / `selfLink` plus
dotted-key bookkeeping with no SIEM value — the Elastic ingest pipeline does
the same). Other IntOrString fields (rolling-update `maxSurge` /
`maxUnavailable`, NetworkPolicy `port`) are not in the dataset we tuned
against; add coercions when they surface as pipeline_error events.

## Enrichment (optional)

The example pipeline does no enrichment — it's a pure parser / ECS
normaliser. Two enrichment paths are common; you can bolt either on
without changing the per-partition remaps. The hooks are isolated, so the
diff stays small.

### Username → canonical identity

Replace `user.name` (the audit event's `user.username`) with a canonical
UPN, and optionally add `user.department`, from your AD / IdP source. The
common Vector idiom:

1. Declare a `memory`
   [enrichment table](https://vector.dev/docs/reference/configuration/global-options/#enrichment_tables.type.memory)
   keyed by `user:<lowercased-sam-or-upn>` (value: canonical UPN) and
   `department:<canonical-upn>` (value: department name).
2. Feed it from your IdP — e.g. an
   [`exec`](https://vector.dev/docs/reference/configuration/sources/exec/)
   source on pod start plus an
   [`http_client`](https://vector.dev/docs/reference/configuration/sources/http_client/)
   refresh, piped through a small `remap` that pivots `{key, value}` rows
   into the table.
3. In each `remap_*` partition, after the existing `user.name` lowercase /
   `oidc:` strip, add a `get_enrichment_table_record("user_mappings", …)`
   block that overwrites `user.name` on hit and sets `user.department`
   from a second lookup. Misses are silent (`if err == null { … }`).

### GeoIP

The audit event's `source.ip` is the client that hit the API server. Most
in-cluster traffic is RFC1918, but `kubectl` from the outside, CI runners,
and federated control planes can be public. To enrich:

1. Declare
   [`geoip`](https://vector.dev/docs/reference/configuration/global-options/#enrichment_tables.type.geoip)
   enrichment tables for the MaxMind City and ASN databases:
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
2. In each `remap_*` partition that sets `source.ip`, gate on
   `is_ipv4(source.ip)` and a private-CIDR skip list (RFC1918, loopback,
   link-local — plus any of your own public ranges) before calling
   `get_enrichment_table_record("geoip_city", { "ip": …})`. Populate
   `source.geo.*` and `source.as.*` per ECS only on lookup success.

## Authorized noise drops

One class of payload is silently dropped — it carries no SIEM signal:

| Drop key | Match | Why |
|---|---|---|
| `noise:proxy_probe` | `.message == "reading PROXY protocol"` | Legacy HAProxy probe line emitted by some upstream collectors; pre-dates the audit JSON shape and never carries an audit event |

The drop is explicit (`abort "noise:proxy_probe"` matched by
`silent_drop_audit`) so a future maintainer can see it and the rationale.
Add a new entry by editing the matching `if … abort "noise:<reason>"` in
the relevant remap.

Anything not on this list reaches a sink — if a real-but-unfamiliar payload
doesn't match a known shape, it surfaces as a `pipeline_error` on stderr.
**Errors are never silently dropped — only noise is.**

## Error contract

Every remap (`parse_audit_line`, `envelope_remap`, all five `remap_*`) runs
with `drop_on_abort: true` and `reroute_dropped: true`. The audit transform
`silent_drop_audit` consumes each remap's `.dropped` output. It silences
only aborts whose reason starts with `noise:` (the table above);
everything else, plus the `route_partitions._unmatched` lane, is converted
into an ECS-conformant `pipeline_error` event with:

- `event.kind = "pipeline_error"`
- `event.category = ["host"]`
- `event.type = ["info","error"]`
- `event.outcome = "failure"`
- `error.type` and `error.message` populated (the `error.message` is the
  exact `noise:<reason>` / abort string)
- `event.original` preserved so the original audit JSON line can be
  replayed

…and emitted on the `stderr_errors` sink.

## Design decisions / why

- **No `if (exists(.x))` guards.** They look defensive but quietly paper
  over real failures. The remaps rely on `?? null`, `string!()`,
  `parse_*!()` and let unexpected shapes route to the audit so they surface
  instead of vanishing.
- **Prefer Vector's built-in functions over hand-rolled regex.**
  `parse_timestamp`, `map_keys`, `ip_cidr_contains` — built-ins are the
  default. Custom string slicing is the last resort.
- **Every meaningful VRL block has a `# ── header ──` comment.** They
  carry the *why*: the audit shape being matched, the K8s-API quirk being
  handled, the regression that motivated a check.
- **Five partitions, not one big remap.** Audit events of different
  `stage` / `level` have meaningfully different field sets (`requestObject`
  only exists at `level: Request`; `objectRef` is absent on non-resource
  API calls; `Panic` events carry a different shape). One remap per
  partition is easier to reason about than one branch-heavy 500-line remap.
- **`_unmatched` is a feature, not a fallback.** Any audit shape that
  doesn't match a partition discriminator surfaces as `pipeline_error` on
  stderr — parser-coverage gaps are visible the moment they appear.
- **Preserve the full audit payload.** The pipeline normalises dotted keys
  and IntOrString fields under `requestObject` / `responseObject` instead
  of stripping the subtrees. Detection rules that look at workload spec,
  RBAC, or container security context (`containers[*].securityContext.privileged`,
  `runAsUser`, capabilities, hostNetwork/hostPID …) get the data they need.
- **The audit transform is generic.** It silences `noise:*` aborts and
  promotes every other abort to a `pipeline_error`. The same shape works
  unmodified for any vendor — only the per-partition remap bodies are
  Kubernetes-specific.

## Customization

- **Different source shape.** If you ship audit events to Vector over HTTP
  (e.g. via Filebeat, Fluent Bit, or a custom shipper), replace the
  `kube_audit` `file` source with `http_server`:

  ```yaml
  sources:
    kube_audit:
      type: http_server
      address: "0.0.0.0:8080"
      decoding:
        codec: json
  ```

  …and remove `parse_audit_line` (the HTTP source decodes the JSON for
  you). Point `envelope_remap.inputs` at the source directly.

- **Different sink.** Replace `stdout_out` with your real sink. Events are
  ECS-shaped JSON; most sinks work without further transforms.

- **Split partitions to different sinks.** The five remaps can fan into
  separate sinks by listing them in different `inputs` arrays — useful when
  the high-volume `metadata_response_complete` traffic wants a different
  retention than the high-SIEM-value `request_response_complete` lane.

- **Add a pipeline-error alarm.** Wire `silent_drop_audit` into a separate
  sink that pages on `event.kind == "pipeline_error"` to catch
  parser-coverage drift the moment a new audit shape appears.

## References

- [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html)
- [Kubernetes — Auditing](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/)
- [Vector — file source](https://vector.dev/docs/reference/configuration/sources/file/)
- [Vector — http_server source](https://vector.dev/docs/reference/configuration/sources/http_server/)
- [Vector — VRL reference](https://vector.dev/docs/reference/vrl/)
- [Vector — enrichment tables (geoip / memory)](https://vector.dev/docs/reference/configuration/global-options/#enrichment_tables)
