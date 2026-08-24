# NEA weather examples

The examples collect Singapore NEA weather data, fsync accepted records to a
local JSONL spool, and send closed windows to Hugging Face. The dataset and
bucket variants share the same collectors, record types, windowing, and replay
behavior.

## Choose an example

| Example | Terminal sink | Configuration |
|---|---|---|
| `nea_weather` | Hugging Face dataset repository | `examples/meathook.toml` |
| `nea_weather_bucket` | Hugging Face storage bucket | `examples/meathook_bucket.toml` |

Both stacks put `JsonlStore` directly before the terminal sink, so an accepted
poll is durable before the pipeline continues.

## Configure and run

1. Put `spool_dir` on persistent storage.
2. Set `HF_TOKEN` to a token with write access to the repository or bucket.
3. Set `X_API_KEY` if the NEA service requires one.
4. Edit the matching TOML configuration.

Run the dataset example:

```bash
HF_TOKEN=hf_... cargo run --example nea_weather -- examples/meathook.toml
```

Run the bucket example:

```bash
HF_TOKEN=hf_... cargo run --features hf-bucket \
  --example nea_weather_bucket -- examples/meathook_bucket.toml
```

The storage bucket must exist before the bucket example starts:

```bash
hf buckets create zeon256/meathook-test --private
```

## Poll intervals and flush windows

The two timing settings do different jobs:

| Setting | Purpose |
|---|---|
| `collectors.<name>.interval` | How often that collector calls the NEA API. |
| `flush.every` | The size of the UTC wall-clock windows stored and uploaded by every pipeline. |

The first poll runs immediately. Later polls run at the configured interval
relative to process startup. Restarting the process starts a new poll schedule;
polls are not aligned to UTC boundaries.

`flush.every` does not start a background timer. It divides time into
Unix-epoch-aligned windows. With the examples' `10m` policy, the windows are:

```text
12:00:00–12:10:00 UTC
12:10:00–12:20:00 UTC
12:20:00–12:30:00 UTC
```

At startup and before every poll, the pipeline calls `Sink::advance()`. That
check uploads any closed windows before collection begins, including when the
following collection fails or returns no records. A UTC boundary closes a
window, but the boundary itself does not run `advance()`; delivery waits for
the next pipeline check.

This distinction matters when the poll interval is longer than the flush
window. Both example configurations poll PM2.5 hourly while using 10-minute
windows. If the process starts at `12:03`:

```text
12:03  Advance, poll PM2.5, and append fresh records to the 12:00 window.
12:10  The 12:00 window closes. No pipeline task runs at this boundary.
13:03  Advance uploads the 12:00 window, then the next poll fills 13:00.
14:03  Advance uploads the 13:00 window, then polling continues.
```

PM2.5 therefore normally produces one occupied window per successful fresh
poll and uploads it at the next hourly check. It does not create empty files
for the intervening 10-minute windows, and the schedule does not become
`1h10m`.

The faster collectors check for closed windows more often:

| Collector | Poll interval | Behavior with `flush.every = "10m"` |
|---|---:|---|
| Air temperature | 1 minute | Polls share each window; delivery usually follows within about one minute. |
| Rainfall | 5 minutes | Polls share each window; delivery usually follows within about five minutes. |
| PM2.5 | 1 hour | One fresh poll usually occupies a window; delivery waits for the next hourly check. |

`flush.max_records` is a separate safety valve. Reaching it sends the current
records before the wall-clock window closes.

## Restarts and shutdown

The examples use `ShutdownPolicy::PreserveActiveWindow`. On graceful shutdown,
closed windows are delivered and the current partial window stays in the JSONL
spool. Restarting within that window appends to the same segment instead of
creating a second one. The stored record count is also restored, so
`max_records` still accounts for records written before the restart.

This is durable because `JsonlStore` is the first and only buffering tier. Do
not put a volatile `MemStore` before it while using
`PreserveActiveWindow`; records still held in memory would not survive exit.

`ShutdownPolicy::FlushAll` also sends the active partial window. Use it when
remote storage must receive current records during shutdown. If a process
using `PreserveActiveWindow` never starts again, its active segment remains
durable locally but is not delivered.

## Local spool and remote files

The spool has one directory per pipeline and one JSONL file per occupied
window:

```text
spool-test/air_temperature/1788134400.jsonl
```

The filename is the UTC window-start Unix timestamp. Successful remote
delivery removes the segment. A rejected delivery leaves it in place for a
later retry.

Dataset and bucket sinks use the same remote path format:

```text
data/{pipeline}/{YYYY-MM-DD}/{HH}-{MM}-{SS}-{content-hash}.parquet
```

For example:

```text
data/air_temperature/2026-08-24/19-20-00-8753a334ba360b8d.parquet
```

The date and time identify the UTC window start. The content fingerprint keeps
multiple physical drains of one window distinct. This can happen when
`max_records` fires or the application explicitly force-flushes a partial
window. Replaying identical records produces the same bytes and path, so a
retry overwrites the same object rather than creating a duplicate.

No remote file is created for an empty window.

To inspect records still held locally:

```bash
find ./spool-test -type f -name '*.jsonl' -print
```

Do not delete an active or rejected segment unless you accept losing those
records.

## Configuration reference

The flush policy is shared by all collectors, while each collector has its own
poll interval:

```toml
spool_dir = "/var/lib/meathook/spool"

[flush]
every = "10m"
max_records = 50_000

[sink.huggingface]
repo = "you/weather-data"
branch = "main"

[collectors.air_temperature]
interval = "1m"

[collectors.rainfall]
interval = "5m"

[collectors.pm25]
interval = "1h"
```

Window assignment uses the time records reach the pipeline, not timestamps
inside the source payload. The examples use the source timestamp together
with the station or region as the deduplication key.
