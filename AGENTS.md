# Agent Notes

## Control Plane Updates

- Do not use `./scripts/install_fugue_ha.sh` to update the existing remote Fugue control plane for normal code changes or verification.
- For this repository, remote control plane updates must go through `git push` to `main` and the GitHub Actions workflow in `.github/workflows/deploy-control-plane.yml`.
- Do not bypass the normal deployment path with manual reinstall or upgrade commands unless the user explicitly asks for that recovery path.
