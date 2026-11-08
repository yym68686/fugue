import { build } from 'esbuild';
import { readFile, writeFile, mkdir, realpath } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const [root, output] = process.argv.slice(2);
if (!root || !output) throw Error('Workers builder requires source and output directories');
const metadata = JSON.parse(await readFile(path.join(root, '.fugue-workers/project.json'), 'utf8'));
const source = await realpath(path.resolve(root, metadata.sourceDir));
const main = await realpath(path.resolve(source, metadata.main));
if (main !== source && !main.startsWith(source + path.sep)) throw Error('Workers main escapes source directory');
if (existsSync(path.join(source, 'package.json'))) {
  const result = spawnSync('npm', [existsSync(path.join(source, 'package-lock.json')) ? 'ci' : 'install', '--no-audit', '--no-fund'], { cwd: source, stdio: 'inherit' });
  if (result.status !== 0) throw Error('Workers npm dependency installation failed');
}
await mkdir(output, { recursive: true });
const request = await readFile(path.join(root, '.fugue-workers/request.mjs'), 'utf8');
const module = metadata.format === 'modules';
const code = module ? `${request}
import worker from ${JSON.stringify(main)};
export * from ${JSON.stringify(main)};
export default {
  ...worker,
  fetch(request, env, ctx) {
    if (typeof worker.fetch !== 'function') throw Error('Workers module must export a fetch handler object');
    return worker.fetch.call(worker, publicRequest(request), env, ctx);
  }
};` : `${request.replace('export function', 'function')}
const nativeAddEventListener = globalThis.addEventListener;
globalThis.addEventListener = (type, handler, ...args) => nativeAddEventListener.call(globalThis, type,
  type !== 'fetch' ? handler : event => handler(new Proxy(event, {
    get(target, key) {
      if (key === 'request') return publicRequest(target.request);
      const value = Reflect.get(target, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    }
  })), ...args);
`;
await build({
  ...(module ? { stdin: { contents: code, resolveDir: source, sourcefile: 'fugue-worker-entry.mjs' } } : { entryPoints: [main], banner: { js: code } }),
  absWorkingDir: source,
  outfile: path.join(output, 'worker.js'),
  bundle: true, format: module ? 'esm' : 'iife', platform: 'neutral',
  conditions: ['workerd', 'worker', 'browser'], mainFields: ['browser', 'module', 'main'],
  external: ['cloudflare:*', 'node:*'], target: 'es2022', logLevel: 'info',
});
delete metadata.main;
delete metadata.sourceDir;
await writeFile(path.join(output, 'project.json'), JSON.stringify(metadata));
