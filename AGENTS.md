# Agent Notes

## Control Plane Updates

- Do not use `./scripts/install_fugue_ha.sh` to update the existing remote Fugue control plane for normal code changes or verification.
- For this repository, remote control plane updates must go through `git push` to `main` and the GitHub Actions workflow in `.github/workflows/deploy-control-plane.yml`.
- Do not bypass the normal deployment path with manual reinstall or upgrade commands unless the user explicitly asks for that recovery path.
- Treat the GitHub Actions deployment workflow and its self-hosted runner as the only normal control-plane release path.
- Do not build images locally and then transfer them to control-plane or cluster nodes over `ssh`, `scp`, `rsync`, `ctr`, `docker save/load`, or similar direct image copy methods.
- Do not patch live control-plane Deployments by hand with ad-hoc image tags when the intended change can be released through the normal workflow.
- If an emergency investigation requires SSH access, keep it read-only by default; only perform manual remote recovery when the user explicitly asks for that exception.
