package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type commandHelpDoc struct {
	Long    string
	Example string
}

var commandHelpDocOverrides = map[string]commandHelpDoc{
	"fugue deploy": {
		Example: strings.TrimSpace(`
fugue deploy .
fugue deploy github owner/repo
fugue deploy github owner/repo --service-env-file gateway=.env.gateway --service-env-file runtime=.env.runtime
fugue deploy github owner/repo --project argus --dry-run
fugue deploy github owner/repo --project argus --replace --wait
`),
	},
	"fugue deploy github": {
		Long: strings.TrimSpace(`
Import a GitHub repository as an app or topology.

When Fugue detects a compose stack or fugue manifest, you can pass --service-env-file repeatedly to inject different env overrides into different topology services without collapsing everything into one shared env file.

Use --dry-run to see the final service/app naming plan before creating anything. Use --update-existing or --replace when you want an existing project topology to update in place instead of creating suffixed copies.
`),
		Example: strings.TrimSpace(`
fugue deploy github owner/repo
fugue deploy github owner/repo --branch main --service-env-file gateway=.env.gateway --service-env-file runtime=.env.runtime
fugue deploy github owner/repo --project argus --dry-run
fugue deploy github owner/repo --project argus --replace --delete-missing
`),
	},
	"fugue app": {
		Example: strings.TrimSpace(`
fugue app overview my-app
fugue app logs runtime my-app --follow
fugue app service attach my-app postgres
`),
	},
	"fugue tenant": {
		Example: strings.TrimSpace(`
fugue tenant ls
fugue tenant ls --output json
`),
	},
	"fugue tenant ls": {
		Long: strings.TrimSpace(`
List the visible tenants/workspaces for the current access key.

Use this when a key can see more than one workspace and you want a direct CLI
answer before choosing --tenant for deploy, project, or app commands.
`),
		Example: strings.TrimSpace(`
fugue tenant ls
fugue tenant ls --output json
`),
	},
	"fugue app overview": {
		Long: strings.TrimSpace(`
Show the app plus related domains, bindings, backing services, operations, image inventory, runtime pod rollout context, and a root-cause diagnosis section in one snapshot.

JSON output redacts env values, passwords, repo tokens, and secret-backed file content by default. Pass --show-secrets only when you explicitly need the raw values for debugging.
`),
		Example: strings.TrimSpace(`
fugue app overview my-app
fugue app overview my-app --output json
fugue app overview my-app --show-secrets --output json
`),
	},
	"fugue app watch": {
		Long: strings.TrimSpace(`
Watch the same aggregated app snapshot as "app overview" and re-render only when the observed state changes.

JSON output redacts env values, passwords, repo tokens, and secret-backed file content by default. Pass --show-secrets only when you explicitly need the raw values for debugging.
`),
		Example: strings.TrimSpace(`
fugue app watch my-app
fugue app watch my-app --interval 10s --show-secrets --output json
`),
	},
	"fugue app logs": {
		Example: strings.TrimSpace(`
fugue app logs runtime my-app --follow
fugue app logs build my-app --tail 200
fugue app logs table my-app --table gateway_request_logs --since 1h --match status=500
fugue app logs pods my-app
`),
	},
	"fugue app logs build": {
		Long: strings.TrimSpace(`
Read build logs and the derived artifact pipeline summary for one app build operation.

Besides the raw log tail, text output shows build, push, publish, deploy, and runtime stages so you can confirm whether the built image was recorded, published to the registry, referenced by deploy, and observed in pods.

When the current key can see cluster diagnostics, the same command also annotates the builder pod identity plus controller and registry evidence so you can distinguish "publish never happened" from "tag existed earlier and later disappeared from registry inventory".
`),
		Example: strings.TrimSpace(`
fugue app logs build my-app
fugue app logs build my-app --operation op_import_123 --tail 500
`),
	},
	"fugue app logs table": {
		Long: strings.TrimSpace(`
Query a business log table through the app effective Postgres connection with semantic time-window and field filters.

Use this when request logs, gateway logs, or audit rows live in the app database and plain runtime log tailing is not enough.
`),
		Example: strings.TrimSpace(`
fugue app logs table my-app --table gateway_request_logs --since 1h --match status=500 --contains path=/admin
fugue app logs table my-app --table request_audit --column created_at --column method --column path --limit 100
`),
	},
	"fugue app logs pods": {
		Long: strings.TrimSpace(`
Show the current pod group plus recent ReplicaSet rollout context for an app so you can see which revision replaced which pod set.

This is the CLI path for inspecting old rollout context when current runtime logs alone no longer explain what changed.
`),
		Example: strings.TrimSpace(`
fugue app logs pods my-app
fugue app logs pods my-app --component postgres
`),
	},
	"fugue app request": {
		Long: strings.TrimSpace(`
Request an app internal HTTP endpoint directly from the control plane and inspect the upstream status, headers, body, and timings.

Use --header-from-env when the app protects internal admin routes with a key that is already present in the effective app env. Use --output-file to keep a shareable local copy of the result. Output is redacted by default; pass --redact=false together with --confirm-raw-output only when you intentionally need raw credentials or response secrets.
`),
		Example: strings.TrimSpace(`
fugue app request my-app /healthz
fugue app request my-app GET /admin/requests --query page=2 --query status=500 --header-from-env X-Service-Key=SERVICE_KEY
`),
	},
	"fugue app request compare": {
		Long: strings.TrimSpace(`
Probe the public route and the internal app service side by side, then let the CLI explain whether the fault lives in public routing, static fallback, auth middleware, or the app itself.

Use --require-env when the endpoint depends on a specific effective app env key and you want Fugue to block early with a concrete missing-key diagnosis instead of returning a bare 4xx/5xx. Use --json or --output-file when you want to hand the comparison result to automation or another engineer. Output is redacted by default; pass --redact=false together with --confirm-raw-output only when you intentionally need raw credentials or response secrets.
`),
		Example: strings.TrimSpace(`
fugue app request compare my-app /healthz
fugue app request compare my-app POST /api/devices --header-from-env Authorization=DEVICE_BOOTSTRAP_TOKEN --require-env PUBLIC_BASE_URL
`),
	},
	"fugue app request stream": {
		Long: strings.TrimSpace(`
Compare public-route and internal-service streaming behavior for one endpoint, including time-to-headers, first body byte, first SSE event, and the first raw chunks or SSE frames.

Use this when an endpoint returns 200 headers but clients still hang waiting for body bytes or SSE events. The CLI will compare Accept */* and Accept text/event-stream by default, preserve keepalive comment frames, and classify whether the stall is inside the app, the public edge, or an external CDN layer.
`),
		Example: strings.TrimSpace(`
fugue app request stream my-app /events
fugue app request stream my-app GET /stream --timeout 15s --accept text/event-stream --json
`),
	},
	"fugue app diagnose": {
		Long: strings.TrimSpace(`
Summarize the most likely runtime root cause for one app by combining pod state, scheduling events, and node pressure signals.

Use this when app request or runtime logs only tell you "connection refused" or "pod pending" but you need the CLI to explain the likely scheduling, eviction, or storage-affinity cause.
`),
		Example: strings.TrimSpace(`
fugue app diagnose my-app
fugue app diagnose my-app --component postgres
`),
	},
	"fugue app command": {
		Example: strings.TrimSpace(`
fugue app command show my-app
fugue app command set my-app --command "python app.py"
fugue app command clear my-app
`),
	},
	"fugue app db": {
		Example: strings.TrimSpace(`
fugue app db show my-app
fugue app db query my-app --sql "select count(*) from gateway_request_logs"
fugue app db configure my-app --database app --user app
fugue app db switchover my-app runtime-b
fugue app db restore plan my-app --source-node node-a --source-pgdata /var/lib/rancher/k3s/storage/pvc-old/pgdata --expected-system-id 7624486791372800022 --table-min-rows users=1
`),
	},
	"fugue app db query": {
		Long: strings.TrimSpace(`
Run a read-only SQL query against the app effective Postgres connection derived from DATABASE_URL or DB_* env, including managed Postgres defaults.

This is the primary CLI path for inspecting business tables and request-log tables without manually execing into the Postgres pod.
`),
		Example: strings.TrimSpace(`
fugue app db query my-app --sql "select count(*) from users"
fugue app db query my-app --sql "select * from gateway_request_logs order by created_at desc limit 50"
`),
	},
	"fugue app db restore": {
		Long: strings.TrimSpace(`
Plan and verify app-owned managed Postgres restores without guessing at Kubernetes storage state.

The plan command is non-mutating and records the source PGDATA, expected PostgreSQL system identifier, and post-restore table checks. The verify command runs read-only SQL checks against the app database after the restore path has been applied.
`),
		Example: strings.TrimSpace(`
fugue app db restore plan my-app --source-node node-a --source-pgdata /var/lib/rancher/k3s/storage/pvc-old/pgdata --expected-system-id 7624486791372800022 --table-min-rows users=1
fugue app db restore verify my-app --expected-database app --table-min-rows users=1
`),
	},
	"fugue app failover": {
		Example: strings.TrimSpace(`
fugue app failover status my-app
fugue app failover policy set my-app --app-to runtime-b
fugue app failover run my-app --to runtime-b
`),
	},
	"fugue app failover policy": {
		Example: strings.TrimSpace(`
fugue app failover policy set my-app --app-to runtime-b --db-to runtime-c
fugue app failover policy clear my-app --db
`),
	},
	"fugue app failover exec": {
		Example: "fugue app failover run my-app --to runtime-b",
	},
	"fugue app source": {
		Example: strings.TrimSpace(`
fugue app source show my-app
fugue app source bind-github my-app owner/repo --branch main
`),
	},
	"fugue app storage": {
		Example: strings.TrimSpace(`
fugue app storage show my-app
fugue app storage set my-app --size 10Gi --mount /data
fugue app storage reset my-app
`),
	},
	"fugue app service": {
		Example: strings.TrimSpace(`
fugue app service ls my-app
fugue app service attach my-app postgres
fugue app service detach my-app postgres
`),
	},
	"fugue app config": {
		Example: strings.TrimSpace(`
fugue app config ls my-app
fugue app config put my-app /app/config.yaml --from-file config.yaml
fugue app config delete my-app /app/config.yaml
`),
	},
	"fugue app fs": {
		Long: strings.TrimSpace(`
Browse either persisted storage mounts or the live runtime filesystem.

Use --source persistent to stay inside the app workspace or persistent storage roots, and use --source live when you want the current container filesystem such as /, /tmp, /etc, or /app.

When filesystem access itself fails, switch to "fugue diagnose fs" so the CLI can classify pod selection, container readiness, exec-path, permission, and missing-path failures for you.
`),
		Example: strings.TrimSpace(`
fugue app fs ls my-app
fugue app fs ls my-app / --source live
fugue app fs put my-app notes/hello.txt --from-file hello.txt
fugue app fs get my-app notes/hello.txt
`),
	},
	"fugue workflow": {
		Long: strings.TrimSpace(`
Run declarative HTTP investigation workflows with step-to-step extraction, per-step timing, and stable machine-readable output.

Use this when you need to reproduce a public API or browser-visible flow from the CLI alone. Workflow files support multiple base URLs, JSON/form/multipart bodies, bearer tokens, cookies, header interpolation, and extraction from response bodies, headers, or cookies.
`),
		Example: strings.TrimSpace(`
fugue workflow run ./signup.yaml
fugue workflow run ./signup.yaml --json
fugue workflow run ./create-project.yaml --output-file ./workflow-result.json
`),
	},
	"fugue workflow run": {
		Long: strings.TrimSpace(`
Execute one YAML or JSON workflow file and emit request summaries, response summaries, extracted variables, status checks, and failure classification for every step.

Use --output-file when you want a shareable local copy of the result. Diagnostic output is redacted by default; pass --redact=false together with --confirm-raw-output only when you intentionally need raw tokens, cookies, or secret-bearing payload fragments.
`),
		Example: strings.TrimSpace(`
fugue workflow run ./signup.yaml
fugue workflow run ./signup.yaml --json
fugue workflow run ./signup.yaml --output-file ./signup-run.json
fugue workflow run ./signup.yaml --redact=false --confirm-raw-output --output-file ./signup-run-raw.json
`),
	},
	"fugue logs": {
		Long: strings.TrimSpace(`
Collect correlated investigation evidence without dropping into separate runtime, builder, or control-plane log tools.

Use the subcommands below when you need one CLI path that gathers the relevant log fragments, snapshots, and timeline context around a failing request, resource, or operation.
`),
		Example: strings.TrimSpace(`
fugue logs collect my-app
fugue logs collect my-app --request-id req_123 --since 30m --json
fugue logs query my-app --request-id req_123 --since 30m --status 200 --json
fugue logs collect my-app --operation op_deploy_123 --output-file ./evidence.json
`),
	},
	"fugue logs collect": {
		Long: strings.TrimSpace(`
Collect workload, build, and control-plane log fragments plus an app/operation timeline into one correlated evidence document.

Use --since and --until when the investigation has a precise time window. Use --request-id, --resource-id, or --operation to narrow the evidence set. Pass --workflow-file when you want Fugue to reproduce a failing flow first and then include that workflow result beside the collected logs. Use --output-file when automation also needs a local evidence JSON artifact.
`),
		Example: strings.TrimSpace(`
fugue logs collect my-app
fugue logs collect my-app --request-id req_123 --since 30m --json
fugue logs collect my-app --operation op_deploy_123 --workflow-file ./signup.yaml --output-file ./evidence.json
`),
	},
	"fugue logs query": {
		Long: strings.TrimSpace(`
Query runtime log entries through the app runtime log stream and normalize them into stable machine-readable fields.

Use this when you need request-id, method, path, status, pod, container, and per-request event correlation in one output instead of grepping raw log text by hand.
`),
		Example: strings.TrimSpace(`
fugue logs query my-app --request-id req_123 --since 30m --json
fugue logs query my-app --method POST --path /chat --status 200 --limit 50
`),
	},
	"fugue debug": {
		Long: strings.TrimSpace(`
Export shareable investigation bundles and bundle-friendly evidence manifests.

Use this when another engineer should be able to continue the analysis from one artifact instead of re-running the whole investigation live.
`),
		Example: strings.TrimSpace(`
fugue debug bundle my-app
fugue debug bundle my-app --request-id req_123 --archive ./bundle.zip --json
`),
	},
	"fugue debug bundle": {
		Long: strings.TrimSpace(`
Create a single zip archive that contains the collected evidence JSON, timeline, snapshots, warnings, and per-source log files for one app investigation.

The bundle uses the same redaction rules as terminal output. Use --archive to control the destination path, and pair --json with --output-file when automation also needs the resulting manifest in a separate machine-readable file.
`),
		Example: strings.TrimSpace(`
fugue debug bundle my-app
fugue debug bundle my-app --request-id req_123 --archive ./bundle.zip --json
fugue debug bundle my-app --since 1h --workflow-file ./signup.yaml --output-file ./bundle-manifest.json
`),
	},
	"fugue api": {
		Example: "fugue api request GET /v1/apps",
	},
	"fugue api request": {
		Long: strings.TrimSpace(`
Send a raw HTTP request to the Fugue control-plane API and show the status line, response headers, server-timing, body, and transport timings.

Use this when you need to inspect a response directly instead of going through the semantic command surface. Use --output-file to mirror the diagnostic result into a local file. Output is redacted by default; pass --redact=false together with --confirm-raw-output only when you explicitly need raw secrets or tokens.
`),
		Example: strings.TrimSpace(`
fugue api request GET /v1/apps
fugue api request POST /v1/apps/app_123/restart
fugue api request PATCH /v1/apps/app_123/env --body '{"set":{"DEBUG":"1"}}'
`),
	},
	"fugue operation": {
		Example: strings.TrimSpace(`
fugue operation ls --app my-app
fugue operation show op_123
fugue operation watch --app my-app
`),
	},
	"fugue operation ls": {
		Long: strings.TrimSpace(`
List operations across all visible apps or narrow the result set to one app, project, operation type, or status.

JSON output redacts desired env values, passwords, and repo tokens by default. Pass --show-secrets only when you explicitly need the raw values for debugging.
`),
		Example: strings.TrimSpace(`
fugue operation ls
fugue operation ls --project marketing --type deploy --status pending
fugue operation ls --app my-app --show-secrets --output json
`),
	},
	"fugue operation show": {
		Long: strings.TrimSpace(`
Show one operation, including the desired source/spec snapshot that produced it.

JSON output redacts desired env values, passwords, and repo tokens by default. Pass --show-secrets only when you explicitly need the raw values for debugging.
`),
		Example: strings.TrimSpace(`
fugue operation show op_123
fugue operation show op_123 --show-secrets --output json
`),
	},
	"fugue operation explain": {
		Long: strings.TrimSpace(`
Explain why an operation is pending, waiting, failed, or otherwise not making progress.

For deploy operations, the diagnosis also inspects the target managed image and the final runtime image so the CLI can say when the deploy already points at a missing release image instead of only reporting queue state.

For builder-placement failures, the diagnosis includes active reservations, active node locks, and a per-node exclusion snapshot so you can see why no builder was chosen without dropping into cluster exec.
`),
		Example: strings.TrimSpace(`
fugue operation explain op_123
fugue operation explain op_123 --show-secrets --output json
`),
	},
	"fugue operation watch": {
		Long: strings.TrimSpace(`
Wait for one operation or the most recent operation for an app until it reaches a terminal state.

JSON output redacts desired env values, passwords, and repo tokens by default. Pass --show-secrets only when you explicitly need the raw values for debugging.
`),
		Example: strings.TrimSpace(`
fugue operation watch op_123
fugue operation watch --app my-app --show-secrets --output json
`),
	},
	"fugue project": {
		Example: strings.TrimSpace(`
fugue project overview marketing
fugue project apps marketing
fugue project watch marketing
fugue project verify marketing --path /healthz
fugue project delete marketing --wait
fugue project images usage marketing
`),
	},
	"fugue project overview": {
		Long: strings.TrimSpace(`
Show the project plus a service-level pipeline view that summarizes build, push, publish, deploy, and runtime status for every app in the topology.

Use this when you want one CLI snapshot instead of manually stitching together project apps, project operations, app build logs, image inventory, and runtime pod state.
`),
		Example: strings.TrimSpace(`
fugue project overview marketing
fugue project overview marketing --output json
`),
	},
	"fugue project watch": {
		Long: strings.TrimSpace(`
Watch the same aggregated project snapshot as "project overview" and re-render the service pipeline only when the observed state changes.
`),
		Example: strings.TrimSpace(`
fugue project watch marketing
fugue project watch marketing --poll --interval 10s
`),
	},
	"fugue project verify": {
		Long: strings.TrimSpace(`
Run basic HTTP checks against the public routes in a project.

This is the fast CLI path for smoke-testing the current route set after a deploy. Use repeated --path flags for multiple endpoints, or --service to scope checks to a subset of topology services.
`),
		Example: strings.TrimSpace(`
fugue project verify marketing
fugue project verify marketing --path / --path /healthz
fugue project verify marketing --service gateway --path /healthz
`),
	},
	"fugue project delete": {
		Long: strings.TrimSpace(`
Delete a project and, by default, cascade cleanup to its apps and remaining backing services.

Use --wait when you want the CLI to keep watching until the project disappears and to show any remaining app/delete-operation cleanup along the way.
`),
		Example: strings.TrimSpace(`
fugue project delete marketing
fugue project delete marketing --wait
fugue project delete marketing --cascade=false
`),
	},
	"fugue project images": {
		Example: "fugue project images usage marketing",
	},
	"fugue runtime": {
		Example: strings.TrimSpace(`
fugue runtime ls
fugue runtime show shared
fugue runtime enroll create edge-a
fugue runtime doctor shared
`),
	},
	"fugue admin runtime access": {
		Example: strings.TrimSpace(`
fugue admin runtime access show shared
fugue admin runtime access set edge-a public
fugue admin runtime access grant edge-a acme
`),
	},
	"fugue admin runtime pool": {
		Example: strings.TrimSpace(`
fugue admin runtime pool show shared
fugue admin runtime pool set edge-a dedicated
`),
	},
	"fugue admin runtime offer": {
		Example: strings.TrimSpace(`
fugue admin runtime offer show edge-a
fugue admin runtime offer set edge-a --cpu 2000 --memory 4096 --storage 50 --monthly-usd 19.99
`),
	},
	"fugue runtime enroll": {
		Example: strings.TrimSpace(`
fugue runtime enroll ls
fugue runtime enroll create edge-a
`),
	},
	"fugue runtime doctor": {
		Long: strings.TrimSpace(`
Inspect runtime status, endpoint, recent heartbeat, and matching cluster nodes in one view.

For managed shared runtimes, doctor also folds in location-specific runtime IDs such as runtime_managed_shared_loc_* so the shared fleet shows real backing nodes during troubleshooting.
`),
		Example: strings.TrimSpace(`
fugue runtime doctor shared
fugue runtime doctor edge-a
`),
	},
	"fugue version": {
		Long: strings.TrimSpace(`
Show the currently running Fugue CLI build version, commit, and build timestamp.

Pass --check-latest when you also want to compare the current binary against the latest released CLI version from GitHub Releases.
`),
		Example: strings.TrimSpace(`
fugue version
fugue version --check-latest
`),
	},
	"fugue upgrade": {
		Long: strings.TrimSpace(`
Download the latest released Fugue CLI archive for the current operating system and architecture, verify its checksum, and replace the currently running binary.

Use --check when you only want to see whether a newer release is available without installing it.
`),
		Example: strings.TrimSpace(`
fugue upgrade --check
fugue upgrade
`),
	},
	"fugue service": {
		Example: strings.TrimSpace(`
fugue service ls
fugue service postgres create app-db --runtime shared
fugue service show app-db
`),
	},
	"fugue service postgres": {
		Example: "fugue service postgres create app-db --runtime shared --database app --user app",
	},
	"fugue admin": {
		Example: strings.TrimSpace(`
fugue admin access api-key create ci --scope app.read --scope app.write
fugue admin cluster status
fugue admin tenant ls
`),
	},
	"fugue admin access": {
		Example: strings.TrimSpace(`
fugue admin access api-key ls
fugue admin access api-key create ci --scope app.read --scope app.write
fugue admin access node-key create edge-a
`),
	},
	"fugue admin access api-key": {
		Example: strings.TrimSpace(`
fugue admin access api-key ls
fugue admin access api-key create ci --scope app.read --scope app.write
fugue admin access api-key rotate ci --scope app.read --scope app.write
`),
	},
	"fugue admin access node-key": {
		Example: strings.TrimSpace(`
fugue admin access node-key ls
fugue admin access node-key create edge-a
fugue admin access node-key usage edge-a
`),
	},
	"fugue admin runtime": {
		Example: strings.TrimSpace(`
fugue admin runtime ls
fugue admin runtime create edge-a --type external-owned --endpoint https://edge.example.com
fugue admin runtime access show runtime-b
fugue admin runtime pool set runtime-b dedicated
`),
	},
	"fugue admin runtime token": {
		Example: strings.TrimSpace(`
fugue admin runtime token ls
fugue admin runtime token create edge-a --ttl 3600
`),
	},
	"fugue admin cluster": {
		Example: strings.TrimSpace(`
fugue admin cluster status
fugue admin cluster node-policy status
fugue admin cluster pods --namespace kube-system
fugue admin cluster dns resolve api.github.com --server 10.43.0.10
fugue admin cluster net websocket my-app --path /ws
`),
	},
	"fugue admin cluster status": {
		Long: strings.TrimSpace(`
Show control-plane deployment health, image versions, and component readiness.

When the API is configured with FUGUE_CONTROL_PLANE_GITHUB_REPOSITORY, this view also includes the latest deploy-control-plane GitHub Actions workflow run so you can correlate control-plane rollouts with the current cluster state.
`),
	},
	"fugue admin cluster pods": {
		Long: strings.TrimSpace(`
List pods across system namespaces and Fugue-managed workloads.

Use --namespace, --node, and --selector to narrow the view during cluster troubleshooting.
`),
	},
	"fugue admin cluster events": {
		Long: strings.TrimSpace(`
List Kubernetes events for system and managed workloads.

Use --namespace, --kind, --name, --type, and --reason to narrow noisy clusters down to the object you are investigating.
`),
	},
	"fugue admin cluster node-policy": {
		Long: strings.TrimSpace(`
Inspect the desired NodePolicy roles alongside the actual Kubernetes labels, taints, health gates, and reconcile drift for joined nodes.

Use this before expanding edge, DNS, or runtime scheduling so node intent and Kubernetes reality are visible from the CLI.
`),
		Example: strings.TrimSpace(`
fugue admin cluster node-policy ls
fugue admin cluster node-policy get ns101351
fugue admin cluster node-policy status
`),
	},
	"fugue admin cluster node-policy ls": {
		Long: strings.TrimSpace(`
List nodes with desired NodePolicy roles, schedulability, Ready and DiskPressure gates, and reconcile state.
`),
		Example: "fugue admin cluster node-policy ls",
	},
	"fugue admin cluster node-policy get": {
		Long: strings.TrimSpace(`
Show one node's desired roles, actual Kubernetes labels and taints, health gate state, and reconcile drift reasons.
`),
		Example: "fugue admin cluster node-policy get ns101351",
	},
	"fugue admin cluster node-policy status": {
		Long: strings.TrimSpace(`
Summarize NodePolicy convergence across the cluster, including unhealthy and unreconciled nodes.
`),
		Example: "fugue admin cluster node-policy status",
	},
	"fugue admin cluster logs": {
		Long: strings.TrimSpace(`
Read logs from any pod in any namespace, including system workloads such as CoreDNS.

This is the CLI entrypoint for cluster-level troubleshooting when app logs are not enough.
`),
		Example: "fugue admin cluster logs --namespace kube-system --pod coredns-abc --container coredns --tail 200",
	},
	"fugue admin cluster exec": {
		Long: strings.TrimSpace(`
Run a one-shot diagnostic command inside a pod in any namespace.

Use this when you need to inspect resolv.conf, run nslookup, curl an upstream, or compare behavior between system pods and app pods. The command retries transient EOF and stream failures by default; use --retries 0 when you need fully single-shot behavior.
`),
		Example: strings.TrimSpace(`
fugue admin cluster exec --namespace kube-system --pod coredns-abc -- cat /etc/resolv.conf
fugue admin cluster exec --namespace app-demo --pod web-abc -- nslookup api.github.com
fugue admin cluster exec --namespace app-demo --pod web-abc --retries 4 --timeout 2m -- sh -lc "psql -c 'select now()'"
`),
	},
	"fugue admin cluster dns": {
		Example: "fugue admin cluster dns resolve api.github.com --server 10.43.0.10",
	},
	"fugue admin cluster dns resolve": {
		Long: strings.TrimSpace(`
Resolve a DNS name from the control plane and optionally pin the query to an explicit DNS server.

This is useful when you need to compare answers across CoreDNS replicas or verify whether a specific server returns stale or broken data.
`),
		Example: strings.TrimSpace(`
fugue admin cluster dns resolve api.github.com
fugue admin cluster dns resolve api.github.com --server 10.43.0.10 --type A
`),
	},
	"fugue admin cluster net": {
		Example: strings.TrimSpace(`
fugue admin cluster net connect api.github.com:443
fugue admin cluster net websocket my-app --path /ws
`),
	},
	"fugue admin cluster net connect": {
		Long: strings.TrimSpace(`
Open a TCP connection from the control plane to a host:port and report the resolved addresses, selected remote address, and elapsed time.

Use this to separate DNS issues from raw reachability and routing problems.
`),
		Example: strings.TrimSpace(`
fugue admin cluster net connect api.github.com:443
fugue admin cluster net connect 91.103.120.48:443 --timeout 5s
`),
	},
	"fugue admin cluster net websocket": {
		Long: strings.TrimSpace(`
Run the same websocket handshake twice: once directly against the app cluster service, and once against the app public route.

Use this when a websocket endpoint returns 502 through the route and you need the CLI to tell you whether the app itself upgrades correctly while the proxy path fails.
`),
		Example: strings.TrimSpace(`
fugue admin cluster net websocket my-app --path /ws
fugue admin cluster net websocket my-app --path "/socket.io/?EIO=4&transport=websocket" --header Cookie=session=abc
`),
	},
	"fugue admin cluster tls": {
		Example: "fugue admin cluster tls probe 104.18.32.47:443 --server-name api.github.com",
	},
	"fugue admin cluster tls probe": {
		Long: strings.TrimSpace(`
Perform a TLS handshake from the control plane and report protocol version, cipher suite, verification status, and peer certificates.

Use --server-name when you need to probe a specific IP with explicit SNI during edge and DNS incident response.
`),
		Example: strings.TrimSpace(`
fugue admin cluster tls probe api.github.com:443
fugue admin cluster tls probe 104.18.32.47:443 --server-name api.github.com
`),
	},
	"fugue admin cluster workload": {
		Example: "fugue admin cluster workload show kube-system deployment coredns",
	},
	"fugue admin cluster node": {
		Example: strings.TrimSpace(`
fugue admin cluster node inspect gcp1
fugue admin cluster node disk gcp1
fugue admin cluster node journal gcp1
`),
	},
	"fugue admin cluster node inspect": {
		Long: strings.TrimSpace(`
Collect the host-level disk totals, largest paths, kubelet journal excerpts, metrics availability signal, and related Kubernetes events for one node.

This is the CLI path for replacing the usual SSH + df + journalctl workflow during disk-pressure and eviction incidents.
`),
		Example: "fugue admin cluster node inspect gcp1",
	},
	"fugue admin cluster node disk": {
		Long: strings.TrimSpace(`
Show the node filesystem totals plus the largest host paths as seen from the node-janitor daemonset, without SSH.
`),
		Example: "fugue admin cluster node disk gcp1",
	},
	"fugue admin cluster node journal": {
		Long: strings.TrimSpace(`
Show recent kubelet eviction and metrics-related journal evidence from the host so you can see why the node started evicting pods or stopped serving stats.
`),
		Example: "fugue admin cluster node journal gcp1",
	},
	"fugue admin cluster node metrics": {
		Long: strings.TrimSpace(`
Explain why node CPU, memory, or storage metrics are present or missing by combining fresh stats/summary probes with host journal evidence.
`),
		Example: "fugue admin cluster node metrics gcp1",
	},
	"fugue admin cluster workload show": {
		Long: strings.TrimSpace(`
Show the normalized workload summary plus the raw manifest snapshot for a Deployment, DaemonSet, StatefulSet, or Pod.

The response includes selectors, node selectors, tolerations, containers, conditions, and currently matching pods so you can inspect rollout wiring without jumping to kubectl.
`),
		Example: strings.TrimSpace(`
fugue admin cluster workload show kube-system deployment coredns
fugue admin cluster workload show fugue-system daemonset node-janitor
`),
	},
	"fugue admin cluster rollout": {
		Example: "fugue admin cluster rollout status kube-system deployment coredns",
	},
	"fugue admin cluster rollout status": {
		Long: strings.TrimSpace(`
Summarize rollout progress for a Kubernetes workload using its desired, updated, ready, and available replica counts plus surfaced conditions.

Use this to confirm whether a system or managed workload has actually rolled out after an image or config change.
`),
		Example: strings.TrimSpace(`
fugue admin cluster rollout status kube-system deployment coredns
fugue admin cluster rollout status fugue-system daemonset topology-labeler
`),
	},
	"fugue admin users": {
		Example: strings.TrimSpace(`
fugue admin users ls
fugue admin users show user@example.com
fugue admin users enrich
`),
	},
	"fugue admin users ls": {
		Long: strings.TrimSpace(`
List the product-layer admin users snapshot served by fugue-web.

This view matches the admin users page shell instead of the lower-level control-plane object model, so it is useful when you are debugging product-layer user state.
`),
	},
	"fugue admin users show": {
		Long: strings.TrimSpace(`
Show one user from the enriched admin users snapshot, including billing and product usage summaries when they are available.
`),
		Example: "fugue admin users show user@example.com",
	},
	"fugue admin users resolve": {
		Long: strings.TrimSpace(`
Resolve one email to the concrete workspace and tenant snapshot that backs that user in fugue-web.

Use this when you know the human email address first and need to jump straight to the matching tenant or workspace without manually scanning the whole tenant list.
`),
		Example: "fugue admin users resolve user@example.com",
	},
	"fugue admin users enrich": {
		Long: strings.TrimSpace(`
Load the fully enriched admin users snapshot with billing and usage overlays from fugue-web.
`),
		Example: "fugue admin users enrich",
	},
	"fugue admin users usage": {
		Long: strings.TrimSpace(`
Load the lighter-weight admin users usage snapshot when you only need service-count and resource-usage summaries.
`),
		Example: "fugue admin users usage",
	},
	"fugue admin billing": {
		Example: strings.TrimSpace(`
fugue admin billing show
fugue admin billing cap --cpu 8000 --memory 16384 --storage 200
fugue admin billing topup 25 --note "manual credit"
`),
	},
	"fugue admin tenant": {
		Example: strings.TrimSpace(`
fugue admin tenant ls
fugue admin tenant create acme
fugue admin tenant delete acme
`),
	},
	"fugue diagnose": {
		Long: strings.TrimSpace(`
Run higher-level troubleshooting workflows that combine multiple low-level probes into one diagnosis.

Use "diagnose timing" when a semantic CLI command feels slow and you need transport timings for every underlying API call. Use "diagnose fs" when workspace or filesystem access fails and you need pod, container, exec-path, and log evidence in one report.
`),
		Example: strings.TrimSpace(`
fugue diagnose timing -- app overview my-app
fugue diagnose fs my-app --path /workspace/data --json
`),
	},
	"fugue diagnose timing": {
		Long: strings.TrimSpace(`
Wrap another Fugue CLI command and capture per-request DNS, connect, TLS, TTFB, total, and server-timing metrics for every HTTP call it makes.

Use this when the semantic command surface works but feels unexpectedly slow and you need to see whether latency is in transport, backend timing, or client-side fan-out. Use --passthrough when you also want the wrapped command output, and use --output-file when you want to save the timing report for later comparison.
`),
		Example: strings.TrimSpace(`
fugue diagnose timing -- app overview my-app
fugue diagnose timing --passthrough -- admin users enrich
`),
	},
	"fugue diagnose fs": {
		Long: strings.TrimSpace(`
Diagnose app filesystem failures by combining app phase, runtime pod selection, container readiness, raw exec errors, recent events, and related log evidence into one report.

Use this when app fs commands or filesystem APIs fail and you need the CLI to tell you whether the problem is pod selection, missing container, container-not-ready, exec stream failure, API unavailability, permissions, or a missing path. Use --output-file to keep the full diagnosis as a local artifact.
`),
		Example: strings.TrimSpace(`
fugue diagnose fs my-app --path /workspace/data
fugue diagnose fs my-app --source persistent --path data --json
fugue diagnose fs my-app --source live --path /tmp --pod web-abc --output-file ./fs-diagnosis.json
`),
	},
	"fugue web": {
		Example: "fugue web diagnose admin-users",
	},
	"fugue web diagnose": {
		Long: strings.TrimSpace(`
Request a fugue-web page snapshot or arbitrary product-layer route and show the raw HTTP response plus transport timings.

Named targets such as admin-users and admin-cluster resolve to the matching fugue-web snapshot routes. Use --output-file to mirror the result into a local file. Output is redacted by default; pass --redact=false together with --confirm-raw-output only when you intentionally need raw cookies, tokens, or secret-bearing payload fragments.
`),
		Example: strings.TrimSpace(`
fugue web diagnose admin-users
fugue web diagnose /api/fugue/admin/pages/users/enrich
fugue web diagnose GET /api/fugue/console/pages/api-keys --cookie 'fugue_session=...'
`),
	},
}

func applyHelpDocs(root *cobra.Command) {
	walkHelpCommands(root, func(cmd *cobra.Command) {
		if shouldSkipHelpDoc(cmd) {
			return
		}
		if override, ok := commandHelpDocOverrides[cmd.CommandPath()]; ok {
			if strings.TrimSpace(cmd.Long) == "" && strings.TrimSpace(override.Long) != "" {
				cmd.Long = override.Long
			}
			if strings.TrimSpace(cmd.Example) == "" && strings.TrimSpace(override.Example) != "" {
				cmd.Example = override.Example
			}
		}
		if strings.TrimSpace(cmd.Long) == "" {
			cmd.Long = buildDefaultCommandLong(cmd)
		}
		if strings.TrimSpace(cmd.Example) == "" {
			cmd.Example = buildDefaultCommandExample(cmd)
		}
	})
}

func walkHelpCommands(cmd *cobra.Command, visit func(*cobra.Command)) {
	visit(cmd)
	for _, child := range cmd.Commands() {
		walkHelpCommands(child, visit)
	}
}

func shouldSkipHelpDoc(cmd *cobra.Command) bool {
	if cmd == nil || cmd.Hidden || hasHiddenParent(cmd) {
		return true
	}
	path := cmd.CommandPath()
	if path == "" {
		return true
	}
	if cmd.Name() == "help" || path == "fugue completion" || strings.HasPrefix(path, "fugue completion ") {
		return true
	}
	return false
}

func hasHiddenParent(cmd *cobra.Command) bool {
	for parent := cmd.Parent(); parent != nil; parent = parent.Parent() {
		if parent.Hidden {
			return true
		}
	}
	return false
}

func buildDefaultCommandLong(cmd *cobra.Command) string {
	short := strings.TrimSpace(cmd.Short)
	if short == "" {
		short = "Use this command"
	}
	if !strings.HasSuffix(short, ".") {
		short += "."
	}
	if hasDocumentedChildren(cmd) {
		return strings.TrimSpace(short + "\n\nUse the subcommands below to continue this workflow.")
	}
	notes := []string{}
	if cmd.Flags().Lookup("show-secrets") != nil {
		notes = append(notes, "JSON output redacts sensitive values by default. Pass --show-secrets only when you explicitly need raw values.")
	}
	notes = append(notes, "The CLI resolves names where possible and supports --output json or --json for machine-readable output.")
	return strings.TrimSpace(short + "\n\n" + strings.Join(notes, "\n\n"))
}

func buildDefaultCommandExample(cmd *cobra.Command) string {
	if hasDocumentedChildren(cmd) {
		if example := buildGroupCommandExamples(cmd); strings.TrimSpace(example) != "" {
			return example
		}
	}
	return buildLeafCommandExample(cmd)
}

func hasDocumentedChildren(cmd *cobra.Command) bool {
	for _, child := range cmd.Commands() {
		if shouldSkipHelpDoc(child) {
			continue
		}
		return true
	}
	return false
}

func buildGroupCommandExamples(cmd *cobra.Command) string {
	lines := make([]string, 0, 3)
	appendLeafExamples := func(command *cobra.Command) {}
	appendLeafExamples = func(command *cobra.Command) {
		if len(lines) >= 3 {
			return
		}
		for _, child := range command.Commands() {
			if shouldSkipHelpDoc(child) {
				continue
			}
			if hasDocumentedChildren(child) {
				appendLeafExamples(child)
				if len(lines) >= 3 {
					return
				}
				continue
			}
			line := strings.TrimSpace(buildLeafCommandExample(child))
			if line == "" {
				continue
			}
			lines = append(lines, line)
			if len(lines) >= 3 {
				return
			}
		}
	}
	appendLeafExamples(cmd)
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func buildLeafCommandExample(cmd *cobra.Command) string {
	path := cmd.CommandPath()
	if override, ok := commandHelpDocOverrides[path]; ok && strings.TrimSpace(override.Example) != "" {
		return strings.TrimSpace(override.Example)
	}
	parts := []string{path}
	if args := sampleUseArgsForCommand(cmd); args != "" {
		parts = append(parts, args)
	}
	if flags := sampleFlagsForCommand(cmd); flags != "" {
		parts = append(parts, flags)
	}
	line := strings.TrimSpace(strings.Join(parts, " "))
	if !strings.Contains(line, "fugue ") {
		return ""
	}
	return line
}

func sampleUseArgsForCommand(cmd *cobra.Command) string {
	fields := strings.Fields(strings.TrimSpace(cmd.Use))
	if len(fields) <= 1 {
		return ""
	}
	path := cmd.CommandPath()
	args := make([]string, 0, len(fields)-1)
	for _, token := range fields[1:] {
		sample := sampleUseToken(path, token)
		if sample == "" {
			continue
		}
		args = append(args, sample)
	}
	return strings.TrimSpace(strings.Join(args, " "))
}

func sampleUseToken(path, token string) string {
	token = strings.TrimSpace(token)
	switch token {
	case "--":
		return "--"
	case "<command...>":
		return "cat /etc/resolv.conf"
	}
	name := strings.Trim(token, "<>[]")
	switch name {
	case "app":
		return "my-app"
	case "service":
		return "postgres"
	case "binding-or-service":
		return "postgres"
	case "project":
		return "marketing"
	case "runtime":
		return "runtime-b"
	case "tenant":
		return "acme"
	case "api-key":
		return "deploy-bot"
	case "node-key":
		return "edge-a"
	case "operation":
		return "op_123"
	case "email":
		return "user@example.com"
	case "hostname":
		return "www.example.com"
	case "repo-or-url":
		return "owner/repo"
	case "path-or-repo":
		return "."
	case "path-or-url":
		return "/v1/apps"
	case "page-or-path":
		return "admin-users"
	case "image-ref":
		return "ghcr.io/example/demo:abc123"
	case "workflow-file":
		return "./signup.yaml"
	case "upload-id":
		return "upload_123"
	case "namespace":
		return "kube-system"
	case "kind":
		return "deployment"
	case "node":
		return "gcp1"
	case "pod":
		return "demo-abc"
	case "target":
		return "api.github.com:443"
	case "name":
		return sampleEntityName(path)
	case "label":
		return sampleLabelName(path)
	case "new-name":
		return sampleRenamedEntity(path)
	case "mode":
		return sampleModeValue(path)
	case "amount":
		return "25"
	case "KEY...":
		return "DEBUG"
	case "KEY=VALUE...":
		return "DEBUG=1"
	case "path":
		return samplePathValue(path)
	case "absolute-path":
		return "/app/config.yaml"
	case "absolute-path...":
		return "/app/config.yaml"
	default:
		return ""
	}
}

func sampleEntityName(path string) string {
	switch {
	case strings.Contains(path, "app create"):
		return "my-app"
	case strings.Contains(path, "project create"):
		return "marketing"
	case strings.Contains(path, "service postgres create"), strings.Contains(path, "service create"):
		return "app-db"
	case strings.Contains(path, "admin runtime create"):
		return "edge-a"
	case strings.Contains(path, "admin tenant create"):
		return "acme"
	default:
		return "demo"
	}
}

func sampleLabelName(path string) string {
	switch {
	case strings.Contains(path, "api-key"):
		return "deploy-bot"
	default:
		return "edge-a"
	}
}

func sampleRenamedEntity(path string) string {
	switch {
	case strings.Contains(path, "project"):
		return "marketing-v2"
	default:
		return "demo-v2"
	}
}

func sampleModeValue(path string) string {
	switch {
	case strings.Contains(path, "pool"):
		return "internal-shared"
	default:
		return "public"
	}
}

func samplePathValue(path string) string {
	switch {
	case strings.Contains(path, " app fs ls"):
		return "/"
	case strings.Contains(path, " app fs put"), strings.Contains(path, " app fs get"), strings.Contains(path, " app fs delete"):
		return "notes/hello.txt"
	case strings.Contains(path, " app fs mkdir"):
		return "notes"
	default:
		return "/workspace"
	}
}

func sampleFlagsForCommand(cmd *cobra.Command) string {
	switch cmd.CommandPath() {
	case "fugue app create":
		return "--github owner/repo --branch main"
	case "fugue app logs":
		return "my-app --follow"
	case "fugue app logs runtime":
		return "--follow"
	case "fugue app logs build":
		return "--tail 200"
	case "fugue app scale":
		return "--replicas 3"
	case "fugue app move":
		return "--to runtime-b"
	case "fugue app command set":
		return "--command \"python app.py\""
	case "fugue app config put":
		return "--from-file config.yaml"
	case "fugue app fs put":
		return "--from-file hello.txt"
	case "fugue api request":
		return "GET /v1/apps"
	case "fugue app db configure":
		return "--database app --user app"
	case "fugue app storage set":
		return "--size 10Gi --mount /data"
	case "fugue app failover exec":
		return "--to runtime-b"
	case "fugue app failover policy set":
		return "--app-to runtime-b --db-to runtime-c"
	case "fugue app failover policy clear":
		return "--db"
	case "fugue project create":
		return "--description \"Landing pages\""
	case "fugue project edit":
		return "--description \"Landing pages\""
	case "fugue service postgres create":
		return "--runtime shared --database app --user app"
	case "fugue admin runtime offer set":
		return "--cpu 2000 --memory 4096 --storage 50 --monthly-usd 19.99"
	case "fugue runtime attach", "fugue runtime enroll create":
		return "--ttl 3600"
	case "fugue admin access api-key update":
		return "--label deploy-bot-v2"
	case "fugue admin access api-key rotate":
		return "--scope app.read --scope app.write"
	case "fugue admin runtime create":
		return "--type external-owned --endpoint https://edge.example.com"
	case "fugue admin runtime token create":
		return "--ttl 3600"
	case "fugue admin billing cap":
		return "--cpu 8000 --memory 16384 --storage 200"
	case "fugue admin billing topup":
		return "--note \"manual credit\""
	case "fugue admin billing set-balance":
		return "--note \"manual correction\""
	case "fugue admin cluster logs":
		return "--namespace kube-system --pod coredns-abc --container coredns --tail 200"
	case "fugue admin cluster exec":
		return "--namespace kube-system --pod coredns-abc -- cat /etc/resolv.conf"
	case "fugue admin cluster dns resolve":
		return "--server 10.43.0.10"
	case "fugue admin cluster net connect":
		return "--timeout 5s"
	case "fugue admin cluster tls probe":
		return "--server-name api.github.com"
	case "fugue admin users show":
		return ""
	case "fugue diagnose timing":
		return "-- app overview my-app"
	case "fugue diagnose fs":
		return "--path /workspace/data --json"
	case "fugue workflow run":
		return "--json"
	case "fugue logs collect":
		return "--request-id req_123 --since 30m --json"
	case "fugue logs query":
		return "--request-id req_123 --since 30m --json"
	case "fugue debug bundle":
		return "--archive ./bundle.zip --json"
	case "fugue web diagnose":
		return "admin-users"
	case "fugue operation ls", "fugue operation watch":
		return "--app my-app"
	}

	flags := []string{}
	if cmd.Flags().Lookup("from-file") != nil {
		flags = append(flags, "--from-file config.yaml")
	}
	if cmd.Flags().Lookup("replicas") != nil && strings.Contains(cmd.CommandPath(), " start") {
		flags = append(flags, "--replicas 1")
	}
	if cmd.Flags().Lookup("scope") != nil {
		flags = append(flags, "--scope app.read")
	}
	if cmd.Flags().Lookup("show-secrets") != nil && (cmd.CommandPath() == "fugue app overview" || strings.HasPrefix(cmd.CommandPath(), "fugue operation ")) {
		flags = append(flags, "--show-secrets")
	}
	return strings.TrimSpace(strings.Join(flags, " "))
}

func documentedCommandPaths(root *cobra.Command) []string {
	paths := make([]string, 0, 64)
	walkHelpCommands(root, func(cmd *cobra.Command) {
		if shouldSkipHelpDoc(cmd) {
			return
		}
		paths = append(paths, cmd.CommandPath())
	})
	return paths
}

func undocumentedCommandsReport(root *cobra.Command) []string {
	missing := make([]string, 0)
	walkHelpCommands(root, func(cmd *cobra.Command) {
		if shouldSkipHelpDoc(cmd) {
			return
		}
		path := cmd.CommandPath()
		if strings.TrimSpace(cmd.Short) == "" {
			missing = append(missing, fmt.Sprintf("%s missing Short", path))
		}
		if strings.TrimSpace(cmd.Long) == "" {
			missing = append(missing, fmt.Sprintf("%s missing Long", path))
		}
		if strings.TrimSpace(cmd.Example) == "" {
			missing = append(missing, fmt.Sprintf("%s missing Example", path))
		}
	})
	return missing
}
