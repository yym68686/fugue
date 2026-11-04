# Bounded automatic image-cache prune receipts

Automatic scheduler passes retain counts, bounded reason summaries, limits,
policy version and the original full-plan hash rather than per-image candidate,
protected, skipped and orphan-blob arrays. Classification and deletion safety
checks still use the complete in-memory plan. A scheduled task keeps its bounded
target payload and binds the persisted receipt ID and original plan hash.

The automatic receipt identity includes node, cluster, runtime, mode and the
full-plan hash. Unchanged observations reuse one row; changed eligibility gets a
different receipt. Active-task, cooldown, capability and global-concurrency
rejections do not append a new plan. Explicit administrator previews remain full
detail and are not replaced by automatic receipts.

This reduces new diagnostic write volume. It neither deletes existing history
nor recovers filesystem space, changes image retention, authorizes additional
deletion, or bounds all control-plane writes. Distinct plans and explicit
previews can still add history. Measure physical relation growth, new detail
bytes and the PostgreSQL volume separately after rollout.
