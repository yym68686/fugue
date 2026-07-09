# Control-plane outage watchdog

Use this runbook when the Fugue control-plane API, Kubernetes API, runner, or control-plane VM appears unavailable.

1. Run the external watchdog from a machine outside the Fugue cluster:

   ```bash
   FUGUE_WATCHDOG_API_URL=https://api.fugue.pro/readyz \
   FUGUE_WATCHDOG_KUBE_API_URL=https://<kube-api>/readyz \
   FUGUE_WATCHDOG_DNS_SERVER=<authoritative-dns-ip> \
   FUGUE_WATCHDOG_DNS_NAME=api.fugue.pro \
   FUGUE_WATCHDOG_EDGE_URLS=https://0-0.pro/ \
   fugue-watchdog -once
   ```

2. If the report shows data-plane probes passing but control-plane probes failing, treat the event as a control-plane outage and leave edge/DNS serving validated LKG.
3. Do not run provider power actions unless the provider API target and operator intent are explicit. The default watchdog provider action mode records evidence only.
4. If provider console/API confirms VM powerdown, record provider action id, request id, operator, result, and UTC timestamps in the incident.
5. After control plane is reachable, run `fugue admin robustness status --json` and verify platform consumers have converged away from stale LKG.
