# Argus managed SSH migration review

Prepared 2026-10-11; explicitly approved and executed the same day. Both managed
identities now serve their original public ports. Only the two approved endpoint
records in `/Users/yanyuming/.ssh/known_hosts` were replaced, with backups.

## Exact proposed identity transition

| Local alias | Existing endpoint | New platform identity fingerprint |
| --- | --- | --- |
| `poly` | `ssh.fugue.pro:22002` | `SHA256:D1U9XQ7oTud3JorLgCG6niNL2Oyarz3thEwZ0zxFNeE` |
| `argus` | `ssh.fugue.pro:22001` | `SHA256:NzpjX49MTVtTVo2ix2NnEwpSbCTeQO5mnqTIQUArx8M` |

The currently served shared image key is
`SHA256:WBoqkEbZghVqM5qMN0tZcSJB1zHLm0kFyrJpbnSyZhY`.
The complete historical sequence that replaced the older client records remains
unproven. Current image-shared identity and missing platform identity ownership
were independently verified; the new identities are separate, persistent,
encrypted records with immutable ownership anchors.

The proposed managed public keys are:

```text
[ssh.fugue.pro]:22002 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAN+Z6ogm0g2xpXtltIkEF2vVY60E4Ut/4mSPpafH1yO
[ssh.fugue.pro]:22001 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAHx+xjoNx4m50zG9eJ1ZpKJbdJ8sdvpJU3AukV+TXUq
```

The current local file has three entries for each endpoint: lines 171/172/173
for port 22002 and 163/165/167 for port 22001. Its reviewed SHA-256 is
`667c0843d22952f87fd8c2342ff8c68f5cfa4fe3f1af93d9048ace03360a7882`.
Line numbers are observations, not write authority. Before any client update,
re-read the exact endpoint records and preserve every unrelated host entry,
file permissions, and a recoverable backup. Do not disable strict host checking.

## Verified execution and access

Both candidate Pod and Service handshakes match their prepared identity. Using
the existing local RSA user key through a controlled private tunnel authenticated
successfully to each candidate and returned its original application hostname,
UID 0, and `/workspace`. The registered user key remains
`sshkey_1782293246_c2a16eb0b051`, fingerprint
`SHA256:SWaGuLxBAvuoiZO8JFNhqJRLnPz0CjvNwZgTE7Wz13Y`.
No test user key is authorized on either Argus candidate.

| Application | Candidate | Original workload Pod UID |
| --- | --- | --- |
| `app_1782466554_f459616ec861` | `ssh-f32600a6b9dd41f43dda119d0e57656f` | `2bf8eb72-35df-4997-9d31-a759770cda5a` |
| `app_1781315778_aa8c2716804e` | `ssh-b4c68a91a8045fd1f0fe70330a7f6c49` | `e2e360b4-0d88-49f6-ad04-534dcd10d8da` |

The application image stays at digest
`sha256:030c7ae8acb2f5698110f05d25a9bf7a6fe24b95227f8e91018430d5d838981b`.
Both original application containers retain their IDs and zero restart counts.
The platform server uses independently released image
`ghcr.io/yym68686/fugue-managed-ssh@sha256:bb45656fabc628cb3b9600919a3d29b4f406eb9a327d784ab775aa3db14da231`.
It executes sessions in the selected application container without changing its
source, image, startup command, or deployment. Session helpers use a private
temporary location and remove themselves after normal completion.

Intent permits PTY, SFTP/modern SCP, and workload-local IPv4/IPv6 loopback ports
1024-65535. It limits each instance to 32 connections, 16 per source IP, and 16
channels per connection. Environment requests are limited to LANG, LC_ALL, and
TERM. General cross-host forwarding is not part of this managed policy. Legacy
`scp -O` is not an accepted compatibility claim.

## Cutover, trust, and recovery

After approval, reserve each existing binding without changing its port, then
activate only a fresh, verified candidate with exact identity, intent, revision,
artifact, and legacy-route preconditions. Recheck all public front paths against
the prepared key, then replace only this Mac's matching endpoint trust records.
Existing SSH connections retain their current backend; new connections use the
platform server. New host identity is an intentional one-time trust transition:
other clients must enroll the same authenticated public keys before their next
connection. This approval would update only the current Mac.

Candidate/public routing failure must not silently change back to an image-shared
host key. Subsequent managed code/configuration rollback retains the same logical
host identity and revalidates current user authorization. Keep old serving
processes and required recovery material until connection and retention checks
allow retirement. The canary demonstrated promotion, reviewed rollback,
reactivation, and a held connection surviving all three operations; revoked
authorization did not return after rollback.

The encrypted records and ownership anchors for all four prepared identities
were restored into an isolated database using an independently retained keyring.
Both Argus identities matched their authenticated API inventory. The protected
dump SHA-256 is `ddd2f2cdf500ffd6ae5052bfc3f48c7719b6316f4585e21ae52705980838fbfa`.
No private key material is included in this review.

Platform-wide front replacement/ACK enrollment and retirement remain separate
work in `managed-ssh-front-rollout.md`. Completing a host-identity cutover does
not, by itself, complete every platform-managed SSH task.

## Approved migration result

The user explicitly approved endpoint selection and this Mac's trust update.
`poly` activated route generation 1 at approximately 12:48 UTC; `argus` activated
route generation 1 at approximately 12:51 UTC. Both preserved their original
public binding IDs and ports. Publication used fresh verified evidence with exact
revision, artifact, intent generation, identity fingerprint, and legacy-route
preconditions. A candidate awaiting its new artifact's runtime observation was
not published until verification completed.

Each new key was independently checked at `ssh.fugue.pro` and all four observed
front addresses (`15.204.94.71`, `51.38.126.103`, `95.169.10.156`,
`154.51.41.196`) before local trust changed. Both ordinary configured aliases
then authenticated using the existing local key with strict host checking and
returned UID 0, `/workspace`, and their respective original application hostname.
No application source, image, startup command, deployment, or user key changed.
Both original business containers retained their IDs and zero restarts.
Repeated login checks remained successful after the cutover. Native forced-PTY
sessions returned `PTY_OK`, and SFTP `pwd` returned `/workspace` on both aliases;
these checks did not write application files.

Owner-only backups:

- `/Users/yanyuming/.ssh/known_hosts.fugue-22002-20261011T124834Z.bak`
  preserves the complete original file, SHA-256
  `667c0843d22952f87fd8c2342ff8c68f5cfa4fe3f1af93d9048ace03360a7882`.
- `/Users/yanyuming/.ssh/known_hosts.fugue-22001-20261011T125117Z.bak`
  preserves the intermediate file after the poly update, SHA-256
  `a24cc1cd88951a39721374a601dc631a2839b925c34d8e6ec38eb1dfa595ee70`.

The resulting file SHA-256 is
`6374f314aed0de767d10ca11d819a59903c3754a2bdd290ba5f0a5ec50ef0b7b`.
Each update required the expected prior file hash, exact matching record set,
unchanged file identity, and preserved permissions. All unrelated records were
retained byte-for-byte. Other clients still need to enroll the new public keys.
