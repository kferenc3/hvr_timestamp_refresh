# HVR Timestamp Refresh

A Python utility script (`main.py`, deployed as `hvrtimestamprefresh.py`) that manages **timestamp-based incremental refresh** for HVR (Fivetran Local Data Processing) replication channels. It acts either as an **AgentPlugin** hooked into HVR's refresh lifecycle, or as a **standalone LDP management tool** that submits and manages refresh jobs via the HVR REST API.

---

## Overview

HVR replication channels can replicate data in full or incrementally. This script implements a watermark-based approach where:

- A **high watermark** marks the upper bound of data to load (typically `now()`)
- A **low watermark** marks the lower bound for incremental loads (previous high watermark minus a small rewind overlap)
- A **state file** on disk persists watermark history between runs
- A **block file** on disk prevents concurrent refreshes from running simultaneously

The script operates in two contexts (`-c`):
- **`agentplugin`** — Runs inside HVR's refresh job lifecycle (hooked via `AgentPlugin` action)
- **`ldp`** — Runs as a management CLI against the HVR LDP REST API

---

## Requirements

- Python 3.x
- [`pyhvr`](https://pypi.org/project/pyhvr/) — HVR Python client library
- [`pytz`](https://pypi.org/project/pytz/) — Timezone support
- Environment: HVR/Fivetran LDP installation with `HVR_CONFIG` set

---

## Installation

The script is intended to be deployed as `hvrtimestamprefresh.py` on the HVR hub host, accessible by the HVR process. When used as an AgentPlugin, HVR invokes it automatically during refresh jobs.

---

## Environment Variables

The script reads the following HVR environment variables at startup:

| Variable | Description |
|---|---|
| `HVR_HUB` | HVR hub name (overridable with `-h`) |
| `HVR_CONFIG` | HVR configuration directory (required; sets the base path for state files) |
| `HVR_JOB_RETRIES` | Number of retries for the current job attempt |
| `HVR_VAR_TS_HIGH_WATERMARK` | High watermark timestamp injected by HVR at runtime |
| `HVR_VAR_TS_LOW_WATERMARK` | Low watermark timestamp injected by HVR at runtime |

For HTTPS connections to the LDP endpoint, `REQUESTS_CA_BUNDLE` may need to point to a CA certificate file.

---

## Usage

```
main.py <mode> <chn> <loc> [userargs]
```

| Positional argument | Description |
|---|---|
| `<mode>` | Operation mode (see modes below) |
| `<chn>` | HVR channel name |
| `<loc>` | HVR location (not used in LDP context but required by AgentPlugin convention) |
| `[userargs]` | Optional string of flags, passed as a single double-quoted argument |

### User Arguments (`userargs`)

| Flag | Description | Required for |
|---|---|---|
| `-c context` | `agentplugin` or `ldp` | Always |
| `-e execution_context` | `initial` or `incremental` | `agentplugin` context |
| `-h hub` | HVR hub name | `ldp` context (if not in env) |
| `-u username` | LDP API username | `ldp` context |
| `-p password` | LDP API password | `ldp` context |
| `-l uri` | URL to the LDP REST endpoint | `ldp` context |
| `-r source_loc` | Source location name (read side) | `ldp refresh` mode |
| `-w target_loc` | Target location name (write side) | `ldp refresh` mode |
| `-d ddl_check` | `yes` to enable DDL mismatch handling | Optional (`ldp`) |
| `-P parallelism` | Number of parallel sessions (default: `6`) | Optional (`ldp`) |
| `-t trace` | Trace verbosity level (`1` or `2`) | Optional |

---

## Modes

### AgentPlugin Modes (`-c agentplugin`)

These modes are triggered automatically by HVR during refresh job execution. They are configured via the `AgentPlugin` channel action with `ExecOnHub=1`.

#### `refr_read_begin`

Runs at the **start of the read phase** of a refresh job.

**What it does:**

1. Logs the current low and high watermarks (or notes that no low watermark is used for initial loads).
2. **Block file check**: If `<channel>.block` exists and `HVR_JOB_RETRIES == 0`, raises an exception — another refresh is already running and this one should not proceed.
3. If no block file exists:
   - Creates the state directory (`$HVR_CONFIG/ts_refresh/<hub>/<channel>/`) if needed.
   - Writes a block file (`<channel>.block`) containing the current process PID: `{"pid": <pid>}`. This acts as a mutex for concurrent refresh protection.
4. **For `incremental` execution context**: Verifies that the state file (`<channel>.refr_state`) exists — an incremental refresh cannot run without prior state from an initial load.
5. **For `initial` execution context**:
   - If no state file exists, creates one: `{"high_watermark": "<ts>", "retries": <n>}`.
   - If a state file already exists, raises an exception — an initial load should not overwrite existing state. Use `reset_state` first.

#### `refr_write_end`

Runs at the **end of the write phase** (after data is successfully written to the target).

**What it does:**

1. Verifies the block file exists (its absence indicates a problem with the refresh flow).
2. **For `incremental` execution context**: Updates the state file with the current watermarks: `{"low_watermark": "<low>", "high_watermark": "<high>"}`.
3. Removes the block file, releasing the mutex for the next refresh cycle.

#### `refr_read_end` / `refr_write_begin`

These modes are no-ops. The script returns immediately. They are part of the AgentPlugin lifecycle but require no action from this script.

---

### LDP Modes (`-c ldp`)

These modes are run manually (or via a scheduler) to manage refresh jobs through the HVR REST API using the `pyhvr` client.

#### `refresh`

Submits a new refresh job to HVR.

**What it does:**

1. **Job state check**: Queries HVR for existing jobs matching the pattern `<channel>-refr-<source_loc>-<target_loc>`.
   - If the job is `FAILED` and a block file exists, the block file is deleted (cleaning up from the previous failed run).
   - If the job is `RUNNING`, raises an exception — a new job cannot start yet.
2. **Block file check**: If a block file is present (and mode is not `remove_block`), raises an exception.
3. **Determines execution context**:
   - If a state file exists → `incremental`
   - If no state file exists → `initial`
4. **Fetches table list** from the HVR channel definition via API.
5. **For `incremental`**:
   - Reads the existing state file to get the previous high watermark.
   - Computes `new_low_watermark = previous_high_watermark - rewind_minutes` (default: 1 minute overlap to avoid gaps).
   - Builds the refresh payload with `ts_low_watermark` and `ts_high_watermark` as context variables.
   - Sets `upsert: True` and `granularity: bulk`.
   - Optionally includes `create_tables` with `recreate_if_mismatch: True` if `-d yes` is set.
   - Saves updated state: `{"high_watermark": <old>, "new_low_watermark": <new_low>, "new_high_watermark": <new_high>, "retries": <n>}`.
6. **For `initial`**:
   - Builds a refresh payload without watermark context variables.
   - Optionally includes `create_tables` settings if `-d yes` is set.
7. Submits the refresh via `POST /hubs/<hub>/channels/<channel>/refresh`.

#### `remove_block`

Forcibly removes the block file, allowing the next refresh to proceed.

Use this when a refresh has failed or was interrupted and the block file was not cleaned up automatically.

#### `reset_state`

Removes both the state file and the block file, resetting the channel to a "never loaded" state.

After reset, the next `refresh` call will perform a full initial load.

#### `prepare_channel`

Configures the HVR channel with the correct `AgentPlugin` and `Restrict` actions for timestamp-based refresh.

**What it does:**

1. Fetches all existing actions on the channel.
2. Removes any existing actions of types `AgentPlugin`, `ColumnProperties`, `TableProperties`, or `Restrict` that are scoped to the `initial`, `incremental`, or `channel_change` contexts. Also removes any `ColumnProperties` action for `hvr_rowid` surrogate keys.
3. Adds the following new actions:

| Scope | Type | Context | Details |
|---|---|---|---|
| `*` (all) | `AgentPlugin` | `initial` | Calls `hvrtimestamprefresh.py -c agentplugin -e initial` on hub |
| `SOURCE` | `Restrict` | `initial` | `{hana_load_timestamp} <= now()` |
| `SOURCE` | `Restrict` | `channel_change` | `{hana_load_timestamp} <= now()` |
| `SOURCE` | `Restrict` | `incremental` | `{hana_load_timestamp} between {hvr_var_ts_low_watermark} and {hvr_var_ts_high_watermark}` |
| `*` (all) | `AgentPlugin` | `incremental` | Calls `hvrtimestamprefresh.py -c agentplugin -e incremental` on hub |

This setup means:
- **Initial load**: pulls all rows with `hana_load_timestamp <= now()`, sets up state.
- **Incremental load**: pulls only rows within the watermark window, with the AgentPlugin managing state transitions before and after.
- **Channel change**: uses the same all-data condition as initial but does not touch state files.

---

## State Files

All state is stored under `$HVR_CONFIG/ts_refresh/<hub>/<channel>/`.

### Block File: `<channel>.block`

```json
{"pid": 12345}
```

Created at `refr_read_begin`, deleted at `refr_write_end`. Prevents concurrent refreshes. Can be manually removed with the `remove_block` mode.

### State File: `<channel>.refr_state`

Tracks watermark history across refresh cycles.

**After initial load:**
```json
{"high_watermark": "2024-01-15T10:00:00", "retries": 0}
```

**After each incremental load:**
```json
{
  "high_watermark": "2024-01-15T10:00:00",
  "new_low_watermark": "2024-01-15T09:59:00",
  "new_high_watermark": "2024-01-15T11:00:00",
  "retries": 0
}
```

The 1-minute rewind (`rewind_minutes = 1`) ensures there is a small overlap between refresh windows to avoid gaps caused by clock skew or in-flight transactions.

---

## Watermark Flow (Incremental Cycle)

```
Previous run:        [low_wm_prev ............. high_wm_prev]
                                          |
                                  1 min rewind
                                          |
New run:                         [new_low_wm ... new_high_wm (now)]
```

1. `new_low_watermark = previous_high_watermark - 1 minute`
2. `new_high_watermark = current timestamp` (from `HVR_VAR_TS_HIGH_WATERMARK` or system time)
3. The overlap prevents data loss if a row was committed just before the previous window closed.

---

## Tracing and Debugging

Set `-t 1` for option-level tracing (logs all configuration values).
Set `-t 2` for detailed environment variable dumping (all `HVR_*` variables).

Passwords and secrets in HVR `!{...}!` format are automatically masked in all output via regex substitution in `print_raw()`.

---

## Example Invocations

### Prepare a channel for timestamp refresh
```bash
python main.py prepare_channel MY_CHANNEL MY_LOC "-c ldp -h MY_HUB -u admin -p secret -l https://lvr-host:4340"
```

### Submit an incremental refresh
```bash
python main.py refresh MY_CHANNEL MY_LOC "-c ldp -h MY_HUB -u admin -p secret -l https://lvr-host:4340 -r SOURCE_LOC -w TARGET_LOC"
```

### Remove a stuck block file
```bash
python main.py remove_block MY_CHANNEL MY_LOC "-c ldp -h MY_HUB -u admin -p secret -l https://lvr-host:4340"
```

### Reset channel state to allow a new initial load
```bash
python main.py reset_state MY_CHANNEL MY_LOC "-c ldp -h MY_HUB -u admin -p secret -l https://lvr-host:4340"
```

### AgentPlugin invocation (called by HVR automatically)
```bash
python main.py refr_read_begin MY_CHANNEL MY_LOC "-c agentplugin -e incremental"
```

---

## Error Handling

All exceptions propagate to `main()`, which catches them, writes the error to `stderr` with the prefix `F_JX0D01:`, flushes all streams, and exits with code `1`. This format is compatible with HVR job failure reporting.

Successful runs exit with code `0`.
