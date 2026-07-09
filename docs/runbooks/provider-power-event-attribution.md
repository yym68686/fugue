# Provider power event attribution

Use this runbook to classify VM power events without guessing.

1. Collect guest-side evidence: `journalctl -b -1`, kernel logs, qemu guest agent shutdown messages, OOM/panic indicators, and systemd stop reasons.
2. Collect provider-side evidence: activity log, maintenance window, power action id, operator/account id, API request id, and status transitions.
3. Classify the event as one of: guest initiated shutdown, hypervisor initiated shutdown, provider maintenance, provider API power action, kernel panic, OOM kill, manual reboot, or unknown power loss.
4. If provider evidence is unavailable, keep the classification `unknown_power_loss` and improve provider log import rather than inventing a cause.
5. Attach provider action ids and timestamps to the robustness incident summary after control plane recovery.
