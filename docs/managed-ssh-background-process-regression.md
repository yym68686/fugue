# Managed SSH background process regression

On 2026-10-12, the desktop remote connection reported `socket hang up` while
native interactive SSH still worked. The installed desktop starts a Unix-socket
application server with `nohup ... </dev/null >log 2>&1 &`, waits for that SSH
command to complete, and opens a second SSH command to proxy the Unix socket.
The desktop log records bootstrap exit 0 followed by proxy exit 1 with
`Connection refused (os error 111)` at 09:34:32 UTC. Strict host-key checking and
public-key authentication succeeded. The target had no listening application
server at that socket. No application image or startup-script change is needed.

The production helper (`sha256:0dcb1b53a69b3667585b0cb9b326deb54759bad17e58f14f23d20178977d4e5f`)
unconditionally sent SIGKILL to the foreground command's entire process group
after `Wait`, including on exit 0. An isolated reproduction using the exact
production image showed a redirected `nohup sleep` surviving ordinary exec but
dying with signal 9 through that helper; changing its process group alone made
it survive. This is a process-lifecycle defect, not an SSH identity, forwarding,
application, or Unix socket permission defect. The previously tested foreground
login, file transfer, and forwarding paths did not cover detached startup.

## Completion and cancellation contract

- A non-PTY command that completes successfully preserves its redirected
  background children. Nonzero exits retain the previous failed-launch cleanup
  behavior and their original status.
- Active-session cancellation, disconnect, authorization expiry/revocation, a
  signaled foreground process, and execution/pipe errors retain process-group
  cleanup. Closed signal/resize channels disable only that input, not context
  cancellation. A regression test exposed the previous premature control-loop
  exit when those channels closed.
- Background children must detach their standard streams. Inherited output
  pipes remain bounded by `WaitDelay`; expiry is an execution error and cleans
  up the group. PTY cleanup and signal delivery remain unchanged.
- A successfully completed background launch is a workload process, not an
  active SSH session. Revoking an SSH grant prevents access and cancels active
  sessions; it is not a retrospective workload-process deletion mechanism.
  Existing `setsid` children already escaped process-group cleanup. This change
  does not add a process supervisor, cross-group kill, or weaker authorization.

## Verification and release

Tests reproduce the defect before the fix, then verify helper shutdown followed
by a separate helper connection to a real background Unix-socket service. They
cover successful completion, active cancellation, closed control channels,
signals, failed launches, and inherited output-pipe limits. The immutable-image acceptance adds
native OpenSSH bootstrap and a second native connection reaching that service
inside an isolated Kubernetes workload, alongside account-shell login,
SCP/SFTP, PTY, loopback forwarding, authorization refresh, retirement, exact
target binding, and unchanged workload checks.

Publish code through the normal `ci.yml` immutable artifact lane. Select only
its successful acceptance receipt in a separate runtime-intent commit; do not
couple code publication to serving configuration or replace a positive LKG
after failed verification. Production validation must preserve application
Pod/container identities, restart counts, SSH host identities, and client trust.
Do not delete user sockets, restart application servers, or reinstall a client
as a speculative workaround. Actual desktop retry and bounded diagnostics must
distinguish bootstrap/proxy failures from transport failures.
