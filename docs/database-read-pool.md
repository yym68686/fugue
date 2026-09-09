# API database connection reuse

`FUGUE_DATABASE_MAX_IDLE_CONNECTIONS` controls how many established PostgreSQL connections each API process retains after concurrent work. Its default is 16. `FUGUE_DATABASE_CONNECTION_MAX_IDLE_TIME` defaults to `5m`; unused connections are closed after that period. These settings do not change the maximum number of concurrent database operations.

Both settings are runtime environment configuration. Setting the idle count to `2` restores the earlier `database/sql` idle-pool behavior without changing code. A count of `0` disables idle retention. Apply configuration through the existing protected configuration release path.

Console snapshot logs report process-wide connection-pool counter deltas during a request. `apps_acquire` and `image_ops_acquire` in `Server-Timing` separately report connection acquisition, including any connection establishment. Query, row transfer and decoding have independent metrics. Counter deltas can include concurrent work and must not be treated as exclusive to one request.
