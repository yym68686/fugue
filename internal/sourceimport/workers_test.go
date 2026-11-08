package sourceimport

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/gorilla/websocket"
)

func workersFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestWorkersDetection(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		files                 map[string]string
		strategy, format, err string
	}{
		{"standalone", map[string]string{"proxy.mjs": `export default { fetch: (req, env) => new Response(env.VALUE) };`}, "workers", "modules", ""},
		{"service", map[string]string{"handler.js": `addEventListener('fetch', event => event.respondWith(new Response('ok')));`}, "workers", "service-worker", ""},
		{"comment", map[string]string{"index.js": `// export default { fetch() {} }
const example = 'addEventListener("fetch", callback)';`}, "nixpacks", "", ""},
		{"node", map[string]string{"package.json": `{"scripts":{"start":"node server.js"}}`, "server.js": `export default { fetch() {} };`}, "buildpacks", "", ""},
		{"docker-precedence", map[string]string{"Dockerfile": "FROM scratch\nEXPOSE 9090", "wrangler.toml": "main = 'index.js'\n[[kv_namespaces]]\nbinding='CACHE'", "index.js": `export default { fetch() {} }`}, "dockerfile", "", ""},
		{"jsonc", map[string]string{"wrangler.jsonc": `{ // comment
"main":"src/index.ts", "compatibility_date":"2026-01-01", "vars":{"URL":"https://example.test/a//b", "OPTIONS":{"enabled":true},},}`, "src/index.ts": `export { default } from './app';`}, "workers", "modules", ""},
		{"toml", map[string]string{"wrangler.toml": "main = 'index.js'\ncompatibility_date = '2026-01-01'\ncompatibility_flags = ['nodejs_compat']\n[vars]\nMESSAGE = 'hello'", "index.js": `export default { fetch() {} };`}, "workers", "modules", ""},
		{"unsupported", map[string]string{"wrangler.toml": "main = 'index.js'\n[[d1_databases]]\nbinding='DB'", "index.js": `export default { fetch() {} };`}, "", "", "unsupported Workers configuration/bindings: d1_databases"},
		{"ambiguous", map[string]string{"one.js": `export default { fetch() {} };`, "two.js": `export default { fetch() {} };`}, "", "", "multiple Workers entrypoints"},
		{"future", map[string]string{"wrangler.json": `{"main":"index.js","compatibility_date":"2099-01-01"}`, "index.js": `export default { fetch() {} };`}, "", "", "newer than runtime"},
		{"escape", map[string]string{"wrangler.json": `{"main":"../outside.js"}`}, "", "", "escapes repository root"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := workersFixture(t, tc.files)
			strategy, dir, _, _, err := detectAutoImportInputs(root, "", "", "")
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("got %v; want %s", err, tc.err)
				}
				return
			}
			if err != nil || strategy != tc.strategy {
				t.Fatalf("strategy=%s err=%v", strategy, err)
			}
			if strategy == model.AppBuildStrategyWorkers {
				p, err := detectWorkersProject(root, dir)
				if err != nil || p.Format != tc.format {
					t.Fatalf("project=%+v err=%v", p, err)
				}
				if tc.name == "jsonc" && (p.SuggestedEnv["URL"] != "https://example.test/a//b" || len(p.JSONVars) != 1) {
					t.Fatalf("JSON variables lost: %+v", p)
				}
			}
		})
	}
}

func TestWorkersOverlayAndSourceBoundary(t *testing.T) {
	root := workersFixture(t, map[string]string{"src/worker.js": `export default { fetch(req) { return new Response(req.url) } };`, "src/wrangler.toml": "main = 'worker.js'"})
	before, _ := os.ReadFile(filepath.Join(root, "src/worker.js"))
	p, files, err := workersBuildOverlay(root, "src")
	if err != nil {
		t.Fatal(err)
	}
	if p.Main != "worker.js" || len(files) < 5 {
		t.Fatalf("incomplete overlay: %+v", p)
	}
	after, _ := os.ReadFile(filepath.Join(root, "src/worker.js"))
	if string(before) != string(after) {
		t.Fatal("source mutated")
	}
	if _, err := os.Stat(filepath.Join(root, workersOverlayDir)); !os.IsNotExist(err) {
		t.Fatal("overlay written into source")
	}
	for _, f := range files {
		if strings.Contains(f.Content, "__WORKERD_VERSION__") {
			t.Fatal("unpinned runtime")
		}
	}
	outside := workersFixture(t, map[string]string{"outside.js": "export default {}"})
	if err := os.Symlink(filepath.Join(outside, "outside.js"), filepath.Join(root, "src/link.js")); err != nil {
		t.Fatal(err)
	}
	if _, err := workersSafePath(filepath.Join(root, "src"), "link.js"); err == nil {
		t.Fatal("accepted escaping symlink")
	}
}

// Run against the real pinned runtime, not an API mock. The CI verification
// lane provisions these npm modules; normal Go tests do not download software.
func TestWorkersRuntime(t *testing.T) {
	modules := os.Getenv("FUGUE_TEST_WORKERS_NODE_MODULES")
	if modules == "" {
		t.Skip("set FUGUE_TEST_WORKERS_NODE_MODULES for workerd integration")
	}
	for _, format := range []string{"modules", "service-worker"} {
		t.Run(format, func(t *testing.T) {
			code := `export default { async fetch(request, env, ctx) {
  if (request.headers.get('Upgrade') === 'websocket') {
    const [client, server] = Object.values(new WebSocketPair()); server.accept();
    server.addEventListener('message', async event => server.send(event.data?.arrayBuffer ? await event.data.arrayBuffer() : event.data));
    return new Response(null, { status:101, webSocket:client });
  }
  return Response.json({url:request.url,host:request.headers.get('host'),origin:request.headers.get('origin'),message:env.MESSAGE,options:env.OPTIONS,secret:env.AUTH_TOKEN});
} };`
			if format == "service-worker" {
				code = `addEventListener('fetch', event => event.respondWith(Response.json({url:event.request.url,host:event.request.headers.get('host'),message:MESSAGE})));`
			}
			root := workersFixture(t, map[string]string{"handler.js": code, "wrangler.jsonc": `{"main":"handler.js","vars":{"MESSAGE":"hello","OPTIONS":{"enabled":true}}}`})
			_, overlay, err := workersBuildOverlay(root, ".")
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range overlay {
				path := filepath.Join(root, f.RelativePath)
				os.MkdirAll(filepath.Dir(path), 0755)
				if err := os.WriteFile(path, []byte(f.Content), 0644); err != nil {
					t.Fatal(err)
				}
			}
			runtime := filepath.Join(root, workersOverlayDir)
			if err := os.Symlink(modules, filepath.Join(runtime, "node_modules")); err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(root, "out")
			build := exec.Command("node", filepath.Join(runtime, "build.mjs"), root, output)
			if out, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build: %v\n%s", err, out)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := listener.Addr().String()
			listener.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			command := exec.CommandContext(ctx, "node", filepath.Join(runtime, "launch.mjs"), "--socket-addr", "http="+addr)
			command.Env = append(os.Environ(), "FUGUE_WORKERS_APP_DIR="+output, "MESSAGE=hello", "OPTIONS={\"enabled\":true}", "AUTH_TOKEN=test-runtime-secret")
			logs, err := os.Create(filepath.Join(root, "runtime.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer logs.Close()
			command.Stdout = logs
			command.Stderr = logs
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { command.Process.Signal(os.Interrupt); command.Wait(); cancel() }()
			client := &http.Client{Timeout: time.Second}
			for until := time.Now().Add(15 * time.Second); ; {
				r, err := client.Get("http://" + addr)
				if err == nil {
					r.Body.Close()
					break
				}
				if time.Now().After(until) {
					data, _ := os.ReadFile(logs.Name())
					t.Fatalf("runtime not ready: %s", data)
				}
				time.Sleep(50 * time.Millisecond)
			}
			for _, authority := range []string{"app.example.test", "app.example.test:8443", "APP.EXAMPLE.TEST:443"} {
				for _, requestPath := range []string{"/path?q=one", "//other.example/path"} {
					req, _ := http.NewRequest("GET", "http://"+addr+requestPath, nil)
					req.Header.Set("X-Fugue-Edge-Route", "app.example.test")
					req.Header.Set("X-Forwarded-Host", authority)
					req.Header.Set("X-Forwarded-Proto", "https")
					req.Header.Set("Origin", "https://app.example.test")
					resp, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					data, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					var body map[string]any
					if err := json.Unmarshal(data, &body); err != nil {
						t.Fatalf("response %d: %s", resp.StatusCode, data)
					}
					publicAuthority := authority
					if authority == "APP.EXAMPLE.TEST:443" {
						publicAuthority = "app.example.test"
					}
					if body["url"] != "https://"+publicAuthority+requestPath || body["host"] != publicAuthority || body["message"] != "hello" {
						t.Fatalf("public request not preserved: %s", data)
					}
					if format == "modules" && (body["secret"] != "test-runtime-secret" || body["origin"] != "https://app.example.test" || body["options"].(map[string]any)["enabled"] != true) {
						t.Fatalf("bindings/Origin not preserved: %s", data)
					}
				}
			}
			if format == "modules" {
				connection, _, err := websocket.DefaultDialer.Dial("ws://"+addr, http.Header{"X-Fugue-Edge-Route": []string{"app.example.test"}, "X-Forwarded-Host": []string{"app.example.test"}, "X-Forwarded-Proto": []string{"https"}})
				if err != nil {
					t.Fatal(err)
				}
				defer connection.Close()
				connection.SetReadDeadline(time.Now().Add(3 * time.Second))
				if err := connection.WriteMessage(websocket.BinaryMessage, []byte("echo")); err != nil {
					t.Fatal(err)
				}
				kind, data, err := connection.ReadMessage()
				if err != nil || kind != websocket.BinaryMessage || string(data) != "echo" {
					t.Fatalf("WebSocket echo: %s %v", data, err)
				}
			}
		})
	}
}
