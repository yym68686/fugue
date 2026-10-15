# Dynamic quality refresh failures

A failed dynamic-quality capture used to replace the complete cache entry with
an empty selection. This discarded successful evidence even when its original
validity window and configuration context still matched. The next compilation
could therefore fall back from physical quality to preserved physical order
because of a transient refresh failure, then switch back on recovery.

Refresh completion now retains the prior selection and exact receipt on failure
only when the configuration digest matches and the original capture is neither
in the future nor expired. The failed attempt remains recorded and logged; it
does not reset the capture timestamp or extend evidence validity. A successful
refresh replaces the prior evidence and clears the failure.

This cache decision grants no publication authority. Publication still
reconstructs the selection from the exact DNS receipt and service evidence,
checks ownership, current candidates, age and scope, and rejects invalid input
without partially mutating the projection. Changed configuration or expired
evidence continues through the existing fallback path. No serving artifact or
runtime configuration is rewritten by the code release.

Regression tests reproduce evidence loss with the previous completion behavior,
exercise repeated failures through the original expiry boundary, and verify
that retained evidence cannot bypass publication validation.
