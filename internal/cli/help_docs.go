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
	"fugue app": {
		Example: strings.TrimSpace(`
fugue app overview my-app
fugue app logs runtime my-app --follow
fugue app service attach my-app postgres
`),
	},
	"fugue app overview": {
		Long: strings.TrimSpace(`
Show the app plus related domains, bindings, backing services, operations, and image inventory in one snapshot.

JSON output redacts env values, passwords, repo tokens, and secret-backed file content by default. Pass --show-secrets only when you explicitly need the raw values for debugging.
`),
		Example: strings.TrimSpace(`
fugue app overview my-app
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
`),
	},
	"fugue app request": {
		Long: strings.TrimSpace(`
Request an app internal HTTP endpoint directly from the control plane and inspect the upstream status, headers, body, and timings.

Use --header-from-env when the app protects internal admin routes with a key that is already present in the effective app env.
`),
		Example: strings.TrimSpace(`
fugue app request my-app /healthz
fugue app request my-app GET /admin/requests --query page=2 --query status=500 --header-from-env X-Service-Key=SERVICE_KEY
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
fugue app db configure my-app --database app --user app --password secret
fugue app db switchover my-app runtime-b
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
		Example: "fugue app source show my-app",
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
`),
		Example: strings.TrimSpace(`
fugue app fs ls my-app
fugue app fs ls my-app / --source live
fugue app fs put my-app notes/hello.txt --from-file hello.txt
fugue app fs get my-app notes/hello.txt
`),
	},
	"fugue api": {
		Example: "fugue api request GET /v1/apps",
	},
	"fugue api request": {
		Long: strings.TrimSpace(`
Send a raw HTTP request to the Fugue control-plane API and show the status line, response headers, server-timing, body, and transport timings.

Use this when you need to inspect a response directly instead of going through the semantic command surface.
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
List operations across all visible apps or narrow the result set to one app with --app.

JSON output redacts desired env values, passwords, and repo tokens by default. Pass --show-secrets only when you explicitly need the raw values for debugging.
`),
		Example: strings.TrimSpace(`
fugue operation ls
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
fugue project images usage marketing
`),
	},
	"fugue project images": {
		Example: "fugue project images usage marketing",
	},
	"fugue runtime": {
		Example: strings.TrimSpace(`
fugue runtime ls
fugue runtime access show shared
fugue runtime doctor shared
`),
	},
	"fugue runtime access": {
		Example: strings.TrimSpace(`
fugue runtime access show shared
fugue runtime access set edge-a public
fugue runtime access grant edge-a acme
`),
	},
	"fugue runtime pool": {
		Example: strings.TrimSpace(`
fugue runtime pool show shared
fugue runtime pool set edge-a dedicated
`),
	},
	"fugue runtime offer": {
		Example: strings.TrimSpace(`
fugue runtime offer show edge-a
fugue runtime offer set edge-a --cpu 2000 --memory 4096 --storage 50 --monthly-usd 19.99
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
fugue admin runtime access runtime-b
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
fugue admin cluster pods --namespace kube-system
fugue admin cluster dns resolve api.github.com --server 10.43.0.10
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
		Example: "fugue admin cluster net connect api.github.com:443",
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
		Example: "fugue diagnose timing -- app overview my-app",
	},
	"fugue diagnose timing": {
		Long: strings.TrimSpace(`
Wrap another Fugue CLI command and capture per-request DNS, connect, TLS, TTFB, total, and server-timing metrics for every HTTP call it makes.

Use this when the semantic command surface works but feels unexpectedly slow and you need to see whether latency is in transport, backend timing, or client-side fan-out.
`),
		Example: strings.TrimSpace(`
fugue diagnose timing -- app overview my-app
fugue diagnose timing --passthrough -- admin users enrich
`),
	},
	"fugue web": {
		Example: "fugue web diagnose admin-users",
	},
	"fugue web diagnose": {
		Long: strings.TrimSpace(`
Request a fugue-web page snapshot or arbitrary product-layer route and show the raw HTTP response plus transport timings.

Named targets such as admin-users and admin-cluster resolve to the matching fugue-web snapshot routes.
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
	if cmd == nil || cmd.Hidden {
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
	notes = append(notes, "The CLI resolves names where possible and supports --output json for machine-readable output.")
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
	case "namespace":
		return "kube-system"
	case "kind":
		return "deployment"
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
		return "--database app --user app --password secret"
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
	case "fugue runtime offer set":
		return "--cpu 2000 --memory 4096 --storage 50 --monthly-usd 19.99"
	case "fugue runtime attach", "fugue runtime enroll create":
		return "--ttl 3600"
	case "fugue admin access api-key update":
		return "--label deploy-bot-v2"
	case "fugue admin access api-key rotate":
		return "--scope app.read --scope app.write"
	case "fugue admin runtime create":
		return "--type external-owned --endpoint https://edge.example.com"
	case "fugue admin runtime share-mode":
		return ""
	case "fugue admin runtime pool-mode":
		return ""
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
