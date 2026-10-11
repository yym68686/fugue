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
