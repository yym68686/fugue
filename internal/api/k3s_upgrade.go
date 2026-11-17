package api

import _ "embed"

//go:embed k3s_upgrade.sh
var guardedK3SUpgradeShell string
