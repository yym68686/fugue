# Filesystem pressure and public DNS metrics

Node filesystem fullness uses kubelet capacity and available bytes. Scheduler
`allocatable` remains the denominator for requested capacity, but is not a
physical disk pressure threshold. `used_bytes` remains the unmodified kubelet
counter: for `imageFs` it can measure image data only. The API reads imageFs
from the kubelet Summary wire location `node.runtime.imageFs`, and falls back
to used/capacity when available bytes are missing or invalid.

The production scrape policy adds `fugue-public-dns`. It follows the endpoints
of the explicitly managed public DNS Services and discovers the selected Pod's
declared HTTP metrics port. It does not scrape UDP/TCP DNS port 53, transport
probe Services, or a candidate merely because the candidate is Running.
Queries for public DNS should use `job="fugue-public-dns"` (also labeled
`serving_role="public"`); legacy Pod metrics remain available for diagnostics.

For this deployment the Prometheus service account already has Endpoints read
access. EndpointSlice read access has not been reconciled into its live RBAC,
so the new job uses the existing Endpoints discovery role. This API is
deprecated but supported by the deployed Kubernetes version. Moving discovery
to EndpointSlice requires its namespace-scoped RBAC to be deployed and tested
first, without widening the account's write permissions. Additional Pod port
discovery is documented in the [Prometheus configuration reference](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#kubernetes_sd_config).

Configuration is released by `ci.yml`'s independent
`observability_configuration` lane. The reconciler validates with the running
promtool, uses ConfigMap resource-version CAS, waits for the mounted config,
then sends SIGHUP to the existing Prometheus process. It verifies fresh public
DNS metrics and compares the loaded healthy targets with the public Services'
selected backend Pods. Neither DNS listeners nor Prometheus Pods/TSDB are
replaced by this configuration change.

Validation includes kubelet JSON decoding, reserved scheduler capacity,
physically full image filesystems with low image-owned bytes, missing
availability, and service backend changes. A live read-only `promtool check
service-discovery` can validate the projected config before publication;
post-publication checks must verify both Services' exact Pod identities rather
than accepting any two healthy DNS instances.
