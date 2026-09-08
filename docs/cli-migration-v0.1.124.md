# CLI migration release v0.1.124

This release preserves existing commands and payloads while adding explicit replacements. Compatibility paths below are scheduled for removal in v0.2.0. Update scripts before upgrading to that breaking release.

## Correctness changes

- `operation wait` now waits independently of TTY and output format. `--timeout` stops local observation without cancelling the server operation.
- HTTP status drives error exit codes. JSON errors remain machine-readable.
- Overview exposes source availability and supports `--require-complete`.
- Embedded application configuration is redacted by default, including policy views. Explicit env export remains unchanged.

## Canonical replacements

| Existing command | Replacement |
| --- | --- |
| `fugue admin runtime pool-mode` | `fugue admin runtime pool set` |
| `fugue admin runtime share` | `fugue admin runtime access grant` |
| `fugue admin runtime share-mode` | `fugue admin runtime access set` |
| `fugue admin runtime unshare` | `fugue admin runtime access revoke` |
| `fugue app binding` | `fugue app service` |
| `fugue app binding attach` | `fugue app service attach` |
| `fugue app binding detach` | `fugue app service detach` |
| `fugue app binding ls` | `fugue app service ls` |
| `fugue app continuity` | `fugue app failover` |
| `fugue app continuity audit` | `fugue app failover status` |
| `fugue app continuity disable` | `fugue app failover policy clear` |
| `fugue app continuity enable` | `fugue app failover policy set` |
| `fugue app continuity show` | `fugue app rollout policy show` |
| `fugue app failover configure` | `fugue app failover policy set` |
| `fugue app failover disable` | `fugue app failover policy clear` |
| `fugue app rebuild` | `fugue app build` |
| `fugue app redeploy` | `fugue app deploy` |
| `fugue app release attempts` | `fugue app release attempt ls` |
| `fugue app release debug-bundle` | `fugue app release attempt bundle` |
| `fugue app release deploy` | `fugue app deploy` |
| `fugue app release explain` | `fugue app release attempt explain` |
| `fugue app release ls` | `fugue app image ls` |
| `fugue app release policy` | `fugue app image retention` |
| `fugue app release policy set` | `fugue app image retention set` |
| `fugue app release policy show` | `fugue app image retention show` |
| `fugue app release prune` | `fugue app image prune` |
| `fugue app release rebuild` | `fugue app build` |
| `fugue app release rollback` | `fugue app rollback` |
| `fugue app release status` | `fugue app release attempt status` |
| `fugue app release tracking` | `fugue app image tracking` |
| `fugue app release tracking diagnose` | `fugue app image tracking diagnose` |
| `fugue app release tracking disable` | `fugue app image tracking disable` |
| `fugue app release tracking history` | `fugue app image tracking history` |
| `fugue app release tracking set` | `fugue app image tracking set` |
| `fugue app release tracking sync` | `fugue app image tracking sync` |
| `fugue app release traffic` | `fugue app traffic set` |
| `fugue app route` | `fugue app domain primary` |
| `fugue app route check` | `fugue app domain primary check` |
| `fugue app route set` | `fugue app domain primary set` |
| `fugue app route show` | `fugue app domain primary verify` |
| `fugue app sync` | `fugue app source sync` |
| `fugue app sync resume` | `fugue app source sync resume` |
| `fugue app sync run` | `fugue app source sync run` |
| `fugue app sync status` | `fugue app source sync status` |
| `fugue app workspace` | `fugue app fs` |
| `fugue app workspace delete` | `fugue app fs delete` |
| `fugue app workspace get` | `fugue app fs get` |
| `fugue app workspace ls` | `fugue app fs ls` |
| `fugue app workspace mkdir` | `fugue app fs mkdir` |
| `fugue app workspace put` | `fugue app fs put` |
| `fugue curl` | `fugue api request` |
| `fugue deploy plan` | `fugue deploy inspect` |
| `fugue deploy plan github` | `fugue deploy inspect github` |
| `fugue domain` | `fugue app domain` |
| `fugue domain add` | `fugue app domain add` |
| `fugue domain check` | `fugue app domain check` |
| `fugue domain delete` | `fugue app domain delete` |
| `fugue domain diagnose` | `fugue app domain diagnose` |
| `fugue domain ls` | `fugue app domain ls` |
| `fugue domain primary` | `fugue app domain primary` |
| `fugue domain primary check` | `fugue app domain primary check` |
| `fugue domain primary set` | `fugue app domain primary set` |
| `fugue domain primary show` | `fugue app domain primary show` |
| `fugue domain primary verify` | `fugue app domain primary verify` |
| `fugue domain repair` | `fugue app domain repair` |
| `fugue domain verify` | `fugue app domain verify` |
| `fugue env` | `fugue app env` |
| `fugue env export` | `fugue app env export` |
| `fugue env generated` | `fugue app env generated` |
| `fugue env generated set` | `fugue app env generated set` |
| `fugue env generated show` | `fugue app env generated show` |
| `fugue env generated unset` | `fugue app env generated unset` |
| `fugue env ls` | `fugue app env ls` |
| `fugue env set` | `fugue app env set` |
| `fugue env unset` | `fugue app env unset` |
| `fugue files` | `fugue app config` |
| `fugue files delete` | `fugue app config delete` |
| `fugue files get` | `fugue app config get` |
| `fugue files ls` | `fugue app config ls` |
| `fugue files put` | `fugue app config put` |
| `fugue files reconcile` | `fugue app config reconcile` |
| `fugue files verify` | `fugue app config verify` |
| `fugue project rename` | `fugue project edit` |
| `fugue project show` | `fugue project overview` |
| `fugue project storage` | `fugue project images usage` |
| `fugue project usage` | `fugue project images usage` |
| `fugue runtime access` | `fugue admin runtime access` |
| `fugue runtime access grant` | `fugue admin runtime access grant` |
| `fugue runtime access revoke` | `fugue admin runtime access revoke` |
| `fugue runtime access set` | `fugue admin runtime access set` |
| `fugue runtime access show` | `fugue admin runtime access show` |
| `fugue runtime attach` | `fugue runtime enroll create` |
| `fugue runtime delete` | `fugue admin runtime delete` |
| `fugue runtime offer` | `fugue admin runtime offer` |
| `fugue runtime offer set` | `fugue admin runtime offer set` |
| `fugue runtime offer show` | `fugue admin runtime offer show` |
| `fugue runtime pool` | `fugue admin runtime pool` |
| `fugue runtime pool set` | `fugue admin runtime pool set` |
| `fugue runtime pool show` | `fugue admin runtime pool show` |
| `fugue service create` | `fugue service postgres create` |
| `fugue template` | `fugue deploy inspect` |
| `fugue template inspect` | `fugue deploy inspect` |
| `fugue template inspect github` | `fugue deploy inspect github` |
| `fugue workspace` | `fugue app fs` |
| `fugue workspace delete` | `fugue app fs delete` |
| `fugue workspace get` | `fugue app fs get` |
| `fugue workspace ls` | `fugue app fs ls` |
| `fugue workspace mkdir` | `fugue app fs mkdir` |
| `fugue workspace put` | `fugue app fs put` |

The command catalog is available offline with `fugue help --all --json`. Export documentation with `fugue help --format markdown`.

Application image inventory and retention use `app image`. Serving versions use `app release versions` and `app release version`; execution attempts use `app release attempt`. Traffic intent uses `app traffic show/set`; observation evidence must be inspected separately.

`app build` retains the existing rebuild workflow, which may also deploy. `app deploy` applies current desired state. `app rollback` retains image rollback semantics; it does not roll back the database or independent configuration.

Source synchronization resumes through `app source sync resume`. Primary-domain path prefixes and live-route verification are available through `app domain primary check/set --path-prefix` and `app domain primary verify`.
