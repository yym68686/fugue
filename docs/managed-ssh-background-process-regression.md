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
signals, failed launches, and inherited output-pipe limits. The immutable-image
acceptance adds native OpenSSH bootstrap and a second native connection reaching that service
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

## Production acceptance on 2026-10-12

- Fix commit `2de587a6` and acceptance/controller-intent commit `3989ecc4` passed
  `make test`, targeted race tests, Linux process tests, and CI `38234766610`.
  The initial run `38233804653` correctly refused the missing explicit controller
  intent before publication. The subsequent intent names the observed immutable
  controller predecessor; no manual production patch was used.
- CI verified the actual amd64 image
  `sha256:23bb759bd7bd41f11ac1052ef446d75b8092c9c9cd477ce41bbfceca0d0c2cad`
  and helper
  `sha256:3a20ec84dff6ddfc6efb25ef76ba15a17147a23a6f510ad80e110545da04721b`,
  including native detached bootstrap, cross-session Unix socket access, and
  enforced network policy. A separate network-disabled local container using
  Codex 0.155.1 and the repaired helper verified bootstrap exit 0, proxy exit 0,
  WebSocket upgrade 101, and an `initialize` response. This local protocol test
  is supplementary, not a substitute for the immutable-image acceptance.
- Runtime generation 10 was independently selected by `0fbb0b34`, through CI
  `38236676636`, using the exact verified receipt and generation-9 predecessor.
  All five managed applications selected the new image, with no candidate left
  pending; their five serving Pods were Ready with zero restarts.
- The real desktop retried automatically. At 10:31:28.300 UTC its log recorded
  `Transport start success`, connection 512; at 10:31:29.981 UTC it recorded
  `initialized=true`, `next=connected`, and `connectionError=null`. The renderer
  cleared the previous error at 10:31:30.029 UTC. No subsequent disconnection was
  recorded through the observation window. The remote control socket was
  listening and the application-server process remained alive. No desktop UI
  automation was used or claimed; acceptance is from the actual client logs and
  live protocol/process evidence, not from an ordinary interactive SSH alone.
- Both application identities remained pinned. The three online front instances
  had fresh ACKs matching all five current routes. Six public paths (two real
  applications, three online edges) passed native login with no remote command,
  Bash command execution, and read-only SFTP. User `known_hosts` retained SHA-256
  `6374f314aed0de767d10ca11d819a59903c3754a2bdd290ba5f0a5ec50ef0b7b`.
- Two sessions opened before rollout survived 102 bounded exchanges over
  27 minutes, closing only on test completion. A separate 265.877-second witness
  on three new-runtime sessions crossed multiple authorization generations;
  minimum sampled remaining lifetime was 179.477 seconds. Both real business
  Pod UIDs, container IDs, and all restart counters were unchanged (zero).
- Five-minute controller observation found no throttling, renewal failure,
  revocation failure, or deadline-exceeded log entry. The new servers' bounded
  audit samples contained no helper-probe, authorization-expiry, or revocation
  failure event. These bounded observations are not a claim that every historical
  authentication rejection has been explained.

BWG remains intentionally offline. Its configured front selector matched the
same 75 historical Pod objects before and after this release (74 Pending, one
Running in API state, not evidence of actual service). Exact-instance public
publication/retirement checks therefore still report missing-or-ambiguous
instance errors and retain old serving slots. The three online fronts serve the
new targets; no missing BWG ACK was forged, skipped for retirement, or used to
delete old state. Secret renewal also temporarily invalidates old digest-bound
evidence until projection and re-verification; the bounded acceptance waited for
fresh positive evidence and independently verified loaded lease renewal. Neither
condition was hidden by weakening safety checks or by changing business Pods.
