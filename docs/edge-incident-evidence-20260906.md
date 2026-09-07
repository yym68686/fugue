# Edge incident evidence and observability follow-up

## Scope

Historical incident window: 2026-09-05 17:58:39 UTC through 2026-09-06 15:58:39 UTC (22 hours). Older errors are excluded. Production validation on September 7 is reported separately and is not counted as historical incident evidence.

## Confirmed defects

- The old Worker reused the broad serving-route predicate for TLS and cache warmups. A DE warmup could initiate ACME for a hostname whose public DNS selected US. In-window CA errors identify the US address and an HTTP challenge 404, followed by failed-authorization rate limits. Commit `7639d6be` restricts warmups to the current route group; `e74ee1f2` declares the existing platform wildcard certificate as a static Worker input. Serving routes remain broader than warmup selection.
- Static Worker initialization allowed a stale host `edge-node.env` credential to override its valid declarative Secret. A direct comparison from the same DE pod returned HTTP 200 with the Secret credential and HTTP 403 with the host credential. The host file declares static workload mode. Commit `97c8866f` retains the deployment credential for static/unknown mode and uses node credentials only for dynamic mode. Tokens are not included in this report.
- Guardian lacked `get` on the explicitly declared wildcard TLS Secret, preventing the normal release. Commit `28db6da2` adds the bounded permission and preserves structured executor failure details.
- The US cohort contains two live nodes. Candidate discovery and activation validation previously assumed one. Commits `112fd3e3` and `4cbc26c3` validate the cohort and compare immutable activation identities. `ebd285f2` declares two Guardian activator nodes; `888b6077` makes the Worker transition require the same two nodes.

## Findings without complete causal attribution

- Caddy's `aborting with incomplete response` samples contain `reading: context canceled`. This establishes cancellation, not who initiated it. Current DNS HTTP probes deliberately read at most 64 KiB, and warmup requests also have explicit limits, but matching User-Agent or source address alone does not prove a historical warning came from one of those probes. Do not suppress these warnings or change response handling on that basis.
- DNS `query_error_count` includes every non-NOERROR result. `ServeDNS` returns REFUSED for a question outside its served zones and NXDOMAIN for an absent in-zone name. These counters are cumulative since process start and are not a 22-hour delta without boundary samples. Existing aggregate evidence cannot attribute each negative answer to a queried name/source or establish that all were expected. No DNS response-policy change is justified by those totals alone.
- A September 7 DNS preflight simulation briefly failed a custom-target route-readiness invariant while the DE credential failure removed its fresh inventory. The same hostname was still served over HTTPS; preflight later passed after DE inventory recovered. The observations do not isolate one complete causal input snapshot. No DNS compiler change was made.

## Observability plan

1. DNS negative responses: emit a bounded structured event with node, process-start identity, zone, normalized question, type, rcode, decision reason (`outside_zone`, `name_absent`, `no_eligible_answer`, `malformed_question`), bundle digest and timestamp. Bound storage by time/ring capacity, rate-limit per reason, report dropped samples, and mask or hash source addresses. Keep qnames and addresses out of metric labels. Preserve existing DNS behavior and expose the data through the OpenAPI-first diagnostic workflow.
2. Probe attribution: add a unique diagnostic probe ID and explicit source to DNS health probes, TLS warmups and cache warmups, propagate them through Front/Caddy/Worker request facts, and emit terminal outcome with bytes read, configured byte limit, EOF, deadline and cancellation cause. Do not trust a client-provided source marker as authorization or automatically exclude arbitrary marked traffic from business metrics.
3. DNS compilation evidence: on invariant failure, persist the exact bounded input identities (app/route intent, node inventory, authority epoch and bundle digests), rejected record and per-host readiness decision. Include whether the record was simulated, published or loaded. Keep positive LKG intact and never turn missing evidence into permission to publish.
4. Credential selection: record only credential source (`deployment_secret` or `node_env`), workload mode and projected Secret version at initialization. Do not log tokens, keys, certificate private material or authorization headers.
5. Time-window reporting: calculate counter increases with process-reset handling and record start/end samples. A lifetime negative-answer counter must not be labeled an incident-window failure count.

Acceptance: correlate a synthetic truncated response end to end; distinguish it from an upstream reset; identify bounded synthetic out-of-zone and absent-name DNS queries; recover an exact compiler rejection snapshot; verify no behavior changes, unbounded label growth, secret leakage, or dependencies from serving configuration to diagnostic availability.

## Production validation

`97c8866f` DE CI run `34110986726` completed successfully at 2026-09-07 10:36:11 UTC. The DE standby rotation `f0fc9818` then completed CI and Guardian verification; both DE Worker slots now use the `f0fc9818` image and Guardian reports stable with local, dependency and route health healthy.

At 10:48 UTC, DNS preflight passed, both public DNS nodes had zero cache load/write and bundle sync errors, all three online edge nodes reported route/TLS readiness, and HTTPS health probes to the DE, US and BWG public addresses returned HTTP 200. Offline DMIT is not counted as an online release target.

US recovery first returned the old failed candidate to the serving LKG. The two-node A/B successor `314b2a66` completed successfully, then `29fe9f46` transferred the explicitly reviewed edge-node credential volume ownership for the inactive B slot. CI run `34119530651` completed successfully; Guardian reports stable with all health layers healthy and both US Worker slots on `29fe9f46`.
