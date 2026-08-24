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
Unix-epoch-aligned windows. The two example configurations intentionally use
different cadences:

| Example | Flush window | Air temperature | Rainfall | PM2.5 |
|---|---:|---:|---:|---:|
| Dataset repository | 10 minutes | 1 minute | 5 minutes | 1 hour |
| Storage bucket | 15 minutes | 5 minutes | 5 minutes | 15 minutes |

At startup and before every poll, the pipeline calls `Sink::advance()`. That
check uploads any closed windows before collection begins, including when the
following collection fails or returns no records. A UTC boundary closes a
window, but the boundary itself does not run `advance()`; delivery waits for
the next pipeline check.

The dataset example demonstrates a poll interval longer than its flush window.
If it starts at `12:03`:

```text
12:03  Poll PM2.5 and append fresh records to the 12:00–12:10 window.
12:10  The window closes, but no pipeline task runs at this boundary.
13:03  Advance uploads 12:00, then the next poll fills the 13:00 window.
```

It normally produces one occupied PM2.5 window per successful fresh poll and
uploads it at the next hourly check. Empty intervening windows do not produce
files, and the schedule does not become `1h10m`.

The bucket example polls PM2.5 every 15 minutes into 15-minute windows. If it
also starts at `12:03`:

```text
12:03  Poll PM2.5 and append fresh records to the 12:00–12:15 window.
12:15  The window closes, but no pipeline task runs at this boundary.
12:18  Advance uploads 12:00, then the next poll fills the 12:15 window.
```

The air-temperature and rainfall collectors check for closed windows more
often because their poll intervals are shorter. `flush.max_records` is a
separate safety valve; reaching it sends the current records before the
wall-clock window closes.

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

The dataset repository configuration uses a shared 10-minute flush policy and
per-collector poll intervals:

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
