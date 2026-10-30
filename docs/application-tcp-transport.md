# Application TCP transport

The production workflow owns `deploy/environments/production/app-tcp-transport/listeners.json`.
It grants an exact public IPv4/TCP socket to an existing managed application,
independently of application image deployment. Declaring a container port in an
app alone does not expose it publicly. The initial declaration is empty.

Each listener records its tenant/app ID, namespace, ManagedApp name and UID,
deployment, node, public address, external/target port and explicit activation.
Code contains no named application, hostname, public address or startup command.
Reserved HTTP/SSH/DNS/control sockets remain owned by their existing transports.

The reconciler verifies the live owner, current deployment cohort, declared
target port, ready local Pod, node address, other Services, host-port declarations
and actual host TCP listeners. It dry-runs every resource before applying any.
The Service uses `externalTrafficPolicy=Local` to preserve the SMTP source IP;
the application must stay on the declared node (e.g. an explicitly selected
managed runtime plus persistent-volume node affinity). A node migration requires
a reviewed declaration/DNS update and independent public acceptance. It does not
automatically substitute another node or forward SMTP through an HTTP edge.

Only a new generation can change transport content. A resource with another
owner, a changed UID, undeclared live edits or a racing resourceVersion is never
adopted or force-applied. A dedicated NetworkPolicy permits only the selected
port to the exact app/workload selector. Its rules combine with the app's own
network policy; keep all other app ingress restricted. `enabled=false` withdraws
the external IP and dedicated allow rule without deleting the workload, spool,
stored mail, PVC or Service identity. This is separate from mail binding
revocation, which fences credentials and SMTP recipient acceptance.

All normal production writes go through a `main` push and `.github/workflows/ci.yml`.
The lane retains resource/digest receipts, containing no application environment
or secrets. Passing Kubernetes checks proves configuration application, not
public reachability, STARTTLS certificate validity, MX/DKIM/SPF/DMARC, queue
durability or email delivery; verify those separately before domain activation.
No IMAP or submission server is created by this transport facility.
