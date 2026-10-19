# Report a rejected DNS successor while retaining the previous configuration

A running DNS backend can keep serving its positive checkpoint while a newer
assignment lacks complete readiness evidence. Previously this path returned the
candidate error without updating its authenticated consumer heartbeat. Repeated
rejections could outlast the heartbeat freshness bound, making the backend's
identity unavailable to DNS decision observation even though its Pod, transport
selection and retained configuration had not changed.

After verifying the successor and rejecting its application, the consumer now
reports `failed` apply and probe status for that exact assignment. The desired
and candidate generations identify the rejected successor; actual and LKG
generations identify the retained checkpoint. This is a negative runtime fact,
not an acknowledgement that the successor is ready.

The current assignment is checked again before reporting, and the loaded state
must still match the observation. Superseded assignments, unselected backends
and cancelled attempts do not report. The existing monotonic cursor, fresh nonce,
evidence hash and authenticated receipt checks apply to failure reports too.
Readiness failures of an already loaded assignment retain their existing path.

The report does not alter the serving state or positive checkpoint. Route proof
expiry, DNS readiness, publication quorum and LKG validity continue to govern
answers and promotion. A failed report remains an error; it cannot grant traffic
authority or extend a retained proof.

The regression starts from a signed, serving checkpoint and rejects successive
current candidates. It checks failure heartbeat verification, distinct nonces
and increasing sequence numbers, retained DNS answers, byte-for-byte checkpoint
preservation and rejection of a concurrently replaced assignment. Existing
transition and recovery tests distinguish negative observations from successful
candidate acknowledgement.
