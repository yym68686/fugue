# Data-plane LKG autonomy

Use this runbook when the control plane is unavailable but edge or DNS nodes are still reachable.

1. Verify edge/DNS nodes are still answering from validated LKG:

   ```bash
   fugue admin robustness status --json
   fugue admin artifact consumers <artifact-id>
   ```

2. Check DNS answers for filtered edge evidence and temporary-filter WAL records on the DNS node.
3. Check edge WAL records for `serve_lkg`, `lkg_write`, and `caddy_reload_lkg`.
4. Keep temporary filters TTL-bound. Do not permanently change route policy while the control plane is unavailable.
5. When the control plane recovers, reconcile consumer generation drift before expanding any platform release.
