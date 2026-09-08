# CLI v0.2.0 migration map

v0.1.124 announced these removals. This table is generated from the R1 command catalog. The historical JSON and flag baseline is in `cli-refactor-audit-2026-09-08/migration-parity.json`.

| Removed path | Replacement | Parameter / behavior notes |
| --- | --- | --- |
| `fugue admin runtime pool-mode` | `fugue admin runtime pool set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue admin runtime share` | `fugue admin runtime access grant` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue admin runtime share-mode` | `fugue admin runtime access set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue admin runtime unshare` | `fugue admin runtime access revoke` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app binding` | `fugue app service` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app binding attach` | `fugue app service attach` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app binding detach` | `fugue app service detach` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app binding ls` | `fugue app service ls` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app continuity` | `fugue app failover` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Move zero-downtime and canary flags to app rollout policy set/clear. Split combined rollout/failover changes into separate requests. |
| `fugue app continuity audit` | `fugue app failover status` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Move zero-downtime and canary flags to app rollout policy set/clear. Split combined rollout/failover changes into separate requests. |
| `fugue app continuity disable` | `fugue app failover policy clear` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Move zero-downtime and canary flags to app rollout policy set/clear. Split combined rollout/failover changes into separate requests. |
| `fugue app continuity enable` | `fugue app failover policy set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Move zero-downtime and canary flags to app rollout policy set/clear. Split combined rollout/failover changes into separate requests. |
| `fugue app continuity show` | `fugue app rollout policy show` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Move zero-downtime and canary flags to app rollout policy set/clear. Split combined rollout/failover changes into separate requests. |
| `fugue app failover configure` | `fugue app failover policy set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Move zero-downtime and canary flags to app rollout policy set/clear. Split combined rollout/failover changes into separate requests. |
| `fugue app failover disable` | `fugue app failover policy clear` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Move zero-downtime and canary flags to app rollout policy set/clear. Split combined rollout/failover changes into separate requests. |
| `fugue app rebuild` | `fugue app build` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app redeploy` | `fugue app deploy` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release attempts` | `fugue app release attempt ls` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release debug-bundle` | `fugue app release attempt bundle` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release deploy` | `fugue app deploy` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release explain` | `fugue app release attempt explain` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release ls` | `fugue app image ls` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release policy` | `fugue app image retention` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release policy set` | `fugue app image retention set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release policy show` | `fugue app image retention show` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release prune` | `fugue app image prune` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release rebuild` | `fugue app build` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release rollback` | `fugue app rollback` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release status` | `fugue app release attempt status` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release tracking` | `fugue app image tracking` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release tracking diagnose` | `fugue app image tracking diagnose` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release tracking disable` | `fugue app image tracking disable` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release tracking history` | `fugue app image tracking history` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release tracking set` | `fugue app image tracking set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release tracking sync` | `fugue app image tracking sync` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app release traffic` | `fugue app traffic set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app route` | `fugue app domain primary` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app route check` | `fugue app domain primary check` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app route set` | `fugue app domain primary set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app route show` | `fugue app domain primary verify` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app sync` | `fugue app source sync` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app sync resume` | `fugue app source sync resume` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app sync run` | `fugue app source sync run` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app sync status` | `fugue app source sync status` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app workspace` | `fugue app fs` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app workspace delete` | `fugue app fs delete` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app workspace get` | `fugue app fs get` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app workspace ls` | `fugue app fs ls` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app workspace mkdir` | `fugue app fs mkdir` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue app workspace put` | `fugue app fs put` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue curl` | `fugue api request` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue deploy plan` | `fugue deploy inspect` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Inspection output mode is inspect; this does not apply a deployment plan. |
| `fugue deploy plan github` | `fugue deploy inspect github` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Inspection output mode is inspect; this does not apply a deployment plan. |
| `fugue domain` | `fugue app domain` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain add` | `fugue app domain add` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain check` | `fugue app domain check` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain delete` | `fugue app domain delete` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain diagnose` | `fugue app domain diagnose` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain ls` | `fugue app domain ls` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain primary` | `fugue app domain primary` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain primary check` | `fugue app domain primary check` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain primary set` | `fugue app domain primary set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain primary show` | `fugue app domain primary show` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain primary verify` | `fugue app domain primary verify` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain repair` | `fugue app domain repair` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue domain verify` | `fugue app domain verify` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env` | `fugue app env` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env export` | `fugue app env export` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env generated` | `fugue app env generated` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env generated set` | `fugue app env generated set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env generated show` | `fugue app env generated show` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env generated unset` | `fugue app env generated unset` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env ls` | `fugue app env ls` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env set` | `fugue app env set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue env unset` | `fugue app env unset` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue files` | `fugue app config` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue files delete` | `fugue app config delete` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue files get` | `fugue app config get` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue files ls` | `fugue app config ls` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue files put` | `fugue app config put` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue files reconcile` | `fugue app config reconcile` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue files verify` | `fugue app config verify` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue project rename` | `fugue project edit` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue project show` | `fugue project overview` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue project storage` | `fugue project images usage` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue project usage` | `fugue project images usage` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime access` | `fugue admin runtime access` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime access grant` | `fugue admin runtime access grant` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime access revoke` | `fugue admin runtime access revoke` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime access set` | `fugue admin runtime access set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime access show` | `fugue admin runtime access show` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime attach` | `fugue runtime enroll create` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime delete` | `fugue admin runtime delete` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime offer` | `fugue admin runtime offer` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime offer set` | `fugue admin runtime offer set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime offer show` | `fugue admin runtime offer show` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime pool` | `fugue admin runtime pool` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime pool set` | `fugue admin runtime pool set` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue runtime pool show` | `fugue admin runtime pool show` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue service create` | `fugue service postgres create` | Remove --type postgres (or omit the default type). Other values remain unsupported. |
| `fugue template` | `fugue deploy inspect` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Inspection output mode is inspect; this does not apply a deployment plan. |
| `fugue template inspect` | `fugue deploy inspect` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Inspection output mode is inspect; this does not apply a deployment plan. |
| `fugue template inspect github` | `fugue deploy inspect github` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. Inspection output mode is inspect; this does not apply a deployment plan. |
| `fugue workspace` | `fugue app fs` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue workspace delete` | `fugue app fs delete` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue workspace get` | `fugue app fs get` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue workspace ls` | `fugue app fs ls` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue workspace mkdir` | `fugue app fs mkdir` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
| `fugue workspace put` | `fugue app fs put` | Positional arguments, flags and defaults are unchanged; the original success JSON object remains available through the replacement. |
