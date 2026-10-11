# Managed SSH account shell regression

The initial production acceptance covered remote commands, forced PTY commands,
SFTP/SCP, and remote development, but missed native interactive login with the
account's configured shell. The user subsequently reported `/bin/sh: 29: Syntax
error: "(" unexpected` after running `ssh poly`.

Read-only inspection found an unambiguous platform defect: the target account
declares `/bin/bash`, while Fugue unconditionally used its `/bin/sh` helper
bootstrap setting for session commands and login shells. The target `/bin/sh`
is Debian dash. The SSH session also had no `SHELL` variable. A Bash array probe
works in the installed Bash and produces the same unexpected-parenthesis error
under dash. Ordinary direct OpenSSH login currently reaches a sh prompt; that
does not reproduce the user's complete terminal startup script or establish
which script was its line 29. Do not infer a change to application startup files.

The fix separates helper staging from session execution. Inside the exact bound
container, the helper resolves the authenticated operating-system account's shell
and home. It starts interactive login with the standard leading-dash `argv[0]`,
uses that same account shell for remote commands, and supplies authoritative
`HOME`, `USER`, `LOGNAME`, and `SHELL`. Account ownership checks, current-user
execution, signed grants, host identities, target binding, and application
images remain unchanged. Invalid declared shells fail capability validation
instead of silently substituting sh.

Tests include Bash-only command syntax with misleading inherited/client shell
variables, absent or ambiguous account metadata, native `ssh -tt` without a
remote command, a real login profile, and a synthetic account whose `/bin/ash`
differs from the bootstrap `/bin/sh`. The native test also retains SCP, SFTP,
forwarding, exact workload identity, and zero-workload-restart assertions.
Production uses the normal immutable-artifact CI followed by an independent
runtime intent selection. No application or local SSH configuration edits are
part of this repair.

The first image-acceptance run completed image publication but then remained in
the opaque verification step without a stage log. That evidence does not identify
which verification command stalled. The verifier now reports bounded stage
timing and exit status. A timeout regression also demonstrates why killing only
a launcher can keep inherited child output pipes open; subprocess verification
now terminates the entire task-owned process group and bounds output cleanup.
No serving behavior is changed based on a guessed CI failure cause.

The new immutable image passed the actual-image acceptance job `114535006986`
in CI run `38161369653`, including `native-interactive-account-login`, SCP/SFTP,
PTY, loopback forwarding, enforced network policy, authorization refresh, and
graceful retirement. Verification completed in 297.9 seconds. Runtime intent
generation 9 selects image
`ghcr.io/yym68686/fugue-managed-ssh@sha256:87541a523fb4bcda86f58b15e33cdfe146989bf86547edbe3bb8184443f46f29`
and helper
`sha256:0dcb1b53a69b3667585b0cb9b326deb54759bad17e58f14f23d20178977d4e5f`.
Its predecessor is the exact independently declared generation 8. This selection
creates verified platform SSH candidates and preserves existing sessions on
retained servers; it does not change business workloads or client host trust.

Production runtime publication succeeded in run `38162033519`, job
`114536778261`. All five managed applications selected the new immutable image.
Plain `ssh poly`, without a remote command, entered the unchanged business
container with `argv[0]=-bash`, `SHELL=/bin/bash`, `HOME=/workspace`, and Bash
5.2.37. A Bash array expression succeeded. Both real applications passed native
interactive login and Bash remote commands independently through all four
public addresses: eight interactive sessions and eight command sessions. The
final public observation was `2026-10-11T18:08:22.967464Z`, receipt SHA-256
`139e9a367f0f014ae1bf1e57b1b1ca5454ad6efe95ebacc08bfdefadfe9a4374`.
Both native SFTP paths remained functional. A held pre-upgrade poly connection
continued exchanging messages after runtime selection. Business Pod/container
IDs and zero restart counts were unchanged, as was the approved known_hosts
SHA-256 `6374f314aed0de767d10ca11d819a59903c3754a2bdd290ba5f0a5ec50ef0b7b`.

The original terminal application's complete integration script was not
available for direct replay, and Warp UI automation was unavailable. The
acceptance above is actual native OpenSSH, not a claimed Warp UI test.
One initial parallel command probe failed before the successful full repeat;
runtime audit also recorded `helper_probe:unavailable` with API exec transport
reset diagnostics. These observations do not establish the transport failure's
root cause or justify weakening capability checks. Existing bounded audit and
runtime diagnostics, plus captured client exit/stdout/stderr in the acceptance
probe, support continued follow-up without a speculative serving change.
