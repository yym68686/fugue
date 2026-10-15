# Host policy task delivery

Node updaters execute one pending task per polling cycle. Periodic image and
volume inventories can therefore keep arriving ahead of host configuration
tasks indefinitely. A live configuration publisher can wait for acknowledgments
while every node continues to heartbeat and complete newer inventory tasks.

Journald and inotify policy tasks waiting at least ten minutes now share the
normal inventory priority. Creation time and task ID retain FIFO ordering within
that priority, so newer periodic inventory cannot continually pass an older
policy task. Fresh policy tasks retain their existing maintenance priority.
Updater upgrades, storage rescue, overdue inventory and deploy-blocking image
copies keep their higher priorities.

Both PostgreSQL and file-backed stores apply the same policy ordering. This
changes delivery order only: task claim checks, node execution preflight,
dry-run behavior and signed configuration semantics remain in force. The
ten-minute threshold is not an execution deadline; higher-priority work or an
offline updater can still delay execution.
