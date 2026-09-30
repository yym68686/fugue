# Inventory epoch format upgrades

A rollback must retain the last positive group publication even after a newer
control binary has stored inventory. Older binaries with strict JSON decoding
cannot read the optional `serving_epoch` observation field. Upgrade those cells
in two independently accepted component releases.

First release the current reader with
`FUGUE_EDGE_CONTROL_DEFER_INVENTORY_EPOCH_WRITE=true`. This is a code-execution
compatibility setting, not a route, membership, or quorum policy. If the durable
inventory has no per-producer epochs, only one original producer envelope with a
minimum of one may continue. The sole observation must be the aggregate's latest
accepted generation, timestamp, slot, code, and topology. Older observations
cannot borrow another producer's fence. Membership expansion and slot changes
are rejected until the second release; positive publication recovery remains
available. No epoch or compatibility flag is added to the old serialized state.

After the reader release is healthy and is the recorded rollback predecessor,
remove the setting in a second component release. A fresh authenticated heartbeat
then writes its original per-producer epoch. Existing positive publications remain
readable while waiting for that heartbeat. Once any original epoch is durable,
the compatibility reader also preserves the extended format on a code rollback;
it never removes evidence to recreate the old format.

The default is the normal per-producer writer. Multi-member cells already using
it need no setting or rollout. Upgrade one cell at a time through the normal
main/CI path, binding the exact deployed predecessor. Check code rollback with
its actual preceding reader, positive LKG recovery, fresh inventory heartbeats,
unchanged serving fence, and public probes before advancing to the next stage.
