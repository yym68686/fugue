// Fugue's edge overwrites these headers. The worker socket is private, behind
// the tenant's normal network boundary. Never change Origin or authentication.
export function publicRequest(request) {
  const route = request.headers.get('X-Fugue-Edge-Route');
  const host = request.headers.get('X-Forwarded-Host');
  // workerd consumes forwardedProtoHeader when constructing request.url.
  const proto = request.headers.get('X-Forwarded-Proto') || new URL(request.url).protocol.slice(0, -1);
  if (!route || !host || !['http', 'https'].includes(proto) ||
      !/^(?:[a-z0-9.-]+|\[[a-f0-9:]+\])(?::[0-9]+)?$/i.test(host)) return request;
  let origin;
  try { origin = new URL(`${proto}://${host}`); } catch { return request; }
  if (origin.username || origin.password ||
      origin.pathname !== '/' || origin.search || origin.hash ||
      !(origin.hostname === route || (route.startsWith('*.') &&
        origin.hostname.endsWith(route.slice(1))))) return request;
  const incoming = new URL(request.url);
  const url = new URL(origin);
  url.pathname = incoming.pathname;
  url.search = incoming.search;
  // Construct the authority as a whole: assigning URL.host alone can retain an
  // internal socket port when the public authority has no explicit port.
  const restored = new Request(url.toString(), request);
  restored.headers.set('Host', origin.host);
  return restored;
}
