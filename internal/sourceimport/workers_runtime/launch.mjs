import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';

const runtime = path.dirname(fileURLToPath(import.meta.url));
const app = process.env.FUGUE_WORKERS_APP_DIR || path.join(runtime, 'app');
const metadata = JSON.parse(await readFile(path.join(app, 'project.json'), 'utf8'));
const quote = JSON.stringify;
const jsonVars = new Set(metadata.jsonVars || []);
const bindings = Object.keys(process.env).sort().map(name => {
  if (jsonVars.has(name)) {
    // Keep Wrangler JSON variable types while allowing separate env updates.
    const value = JSON.stringify(JSON.parse(process.env[name]));
    return `(name = ${quote(name)}, json = ${quote(value)})`;
  }
  return `(name = ${quote(name)}, fromEnvironment = ${quote(name)})`;
});
const script = quote('worker.js');
const worker = metadata.format === 'modules'
  ? `modules = [(name = "worker.js", esModule = embed ${script})]`
  : `serviceWorkerScript = embed ${script}`;
const config = `using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
  services = [(name = "main", worker = (
    ${worker},
    compatibilityDate = ${quote(metadata.compatibilityDate)},
    compatibilityFlags = [${(metadata.compatibilityFlags || []).map(quote).join(',')}],
    bindings = [${bindings.join(',')}]
  ))],
  sockets = [(name = "http", address = "*:8080", http = (
    forwardedProtoHeader = "X-Forwarded-Proto"
  ), service = "main")]
);`;
const temporary = await mkdtemp(path.join(tmpdir(), 'fugue-workers-'));
await writeFile(path.join(temporary, 'worker.js'), await readFile(path.join(app, 'worker.js')));
await writeFile(path.join(temporary, 'config.capnp'), config, { mode: 0o600 });
const child = spawn(path.join(runtime, 'node_modules/.bin/workerd'), ['serve', path.join(temporary, 'config.capnp'), ...process.argv.slice(2)], { stdio: 'inherit' });
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => child.kill(signal));
child.on('error', async error => { console.error(error.message); await rm(temporary, { recursive: true, force: true }); process.exit(1); });
child.on('exit', async (code, signal) => { await rm(temporary, { recursive: true, force: true }); process.exit(code ?? (signal === 'SIGTERM' ? 0 : 1)); });
