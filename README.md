# HVR Timestamp Refresh

A CLI helper script for HVR 6 that automates incremental and initial refresh jobs on a channel using a timestamp-based low-watermark strategy. The script queries the target database for the maximum replicated timestamp, manages HVR `Restrict` actions, creates a refresh job via the HVR API, and maintains a state file to track progress across runs.

---

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Dependencies](#dependencies)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Arguments Reference](#arguments-reference)
- [State and Block Files](#state-and-block-files)
- [Cron Scheduling](#cron-scheduling)
- [Troubleshooting](#troubleshooting)

---

## Overview

`hvr_timestamp_refresh.py` enables timestamp-driven incremental refresh for any ODBC-reachable source database replicated through HVR 6. On each run the script:

1. Determines the maximum replicated timestamp from the target table via ODBC.
2. Sets the appropriate HVR `Restrict` action on the channel so only rows newer than the watermark are replicated.
3. Creates a refresh job (initial or incremental) through the HVR API.
4. Monitors the job until it reaches a terminal state.
5. Updates the state file with the new low-watermark for the next run.

A **block file** prevents concurrent or overlapping refresh attempts. The block is automatically removed on successful job completion.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│  1. Query target DB  →  MAX(ts_column) FROM target_table        │
│  2. Update Restrict action  →  ts_column > {hvr_var_ts_low_watermark} │
│  3. Determine context  →  initial (no state file) or incremental │
│  4. Create HVR refresh job with context_variables               │
│  5. Create block file                                           │
│  6. Poll job status until PENDING / FAILED / RETRY              │
│  7. On success: update state file, remove block file            │
└─────────────────────────────────────────────────────────────────┘
```

### Execution Contexts

| Context | Triggered when | Behaviour |
|---|---|---|
| `initial` | State file absent **or** target table empty | Full refresh, no timestamp filter |
| `incremental` | State file present and target has rows | Filtered refresh using `ts_low_watermark` variable |

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.9 |
| HVR | 6.x |
| ODBC driver | Matching your target database |
| `uv` (package manager) | Latest recommended |

The target database must be reachable via an ODBC DSN configured on the host running this script.

---

## Dependencies

| Package | Version | Source | Purpose |
|---|---|---|---|
| `pyhvr` | latest | [GitHub (kferenc3/pyhvr)](https://github.com/kferenc3/pyhvr) | HVR 6 REST API client |
| `pyodbc` | ≥ 5.3.0 | PyPI | ODBC connectivity to the target database |
| `dotenv` | ≥ 0.9.9 | PyPI | Load environment variables from a `.env` file |
| `pytz` | ≥ 2026.1 | PyPI | Timezone utilities |

> **Note:** `pyhvr` is sourced from a Git repository and is not available on PyPI. Internet access or a local mirror is required during installation.

### System-level ODBC

In addition to the Python packages, an ODBC driver manager and a database-specific driver must be installed on the host:

**Linux (unixODBC)**
```bash
# RHEL / CentOS / Rocky
sudo dnf install unixODBC unixODBC-devel

# Debian / Ubuntu
sudo apt-get install unixodbc unixodbc-dev
```

Configure your DSN in `/etc/odbc.ini` (system-wide) or `~/.odbc.ini` (user-level):

```ini
[MY_TARGET_DSN]
Driver   = /path/to/driver.so
Server   = db-host
Database = mydb
Port     = 1433
```

---

## Installation

### 1. Clone the repository

```bash
git clone <repository-url> hvr_timestamp_refresh
cd hvr_timestamp_refresh
```

### 2. Install `uv` (if not already installed)

```bash
curl -Lso - https://astral.sh/uv/install.sh | sh
```

### 3. Create the virtual environment and install dependencies

```bash
uv sync
```

This reads `pyproject.toml`, creates `.venv/`, and installs all pinned dependencies from `uv.lock`.

### 4. Activate the virtual environment

```bash
source .venv/bin/activate
```

Or run the script directly through the venv interpreter:

```bash
.venv/bin/python hvr_timestamp_refresh.py ...
```

### 5. Create a `.env` file

Copy the example below and populate with your values (see [Configuration](#configuration)):

```bash
cp .env.example .env   # if an example exists, otherwise create it manually
```

---

## Configuration

The script reads connection details and runtime behaviour from **environment variables**. These can be set in the shell or in a `.env` file in the project root (loaded automatically via `python-dotenv`).

| Variable | Required | Description |
|---|---|---|
| `HVR_CONFIG` | Yes | Path to the HVR configuration directory (e.g. `/opt/hvr/config`). State and block files are stored under `$HVR_CONFIG/ts_refresh/<hub>/<channel>/`. |
| `HVR_HUB` | Yes | HVR hub name. |
| `HVR_URI` | Yes | Base URL of the HVR REST API (e.g. `https://hvr-host:4340/`). |
| `HVR_USERNAME` | Yes | HVR API username. |
| `HVR_PASSWORD` | Yes | HVR API password. |
| `HVR_JOB_RETRIES` | No | Number of retries for the refresh job (default: `0`). |
| `HVR_VAR_TS_LOW_WATERMARK` | No | Override low-watermark timestamp. Normally derived automatically from the target. |
| `HVR_VERIFY_SSL` | No | Set to `false` to disable SSL certificate verification (default: `true`). |
| `REQUESTS_CA_BUNDLE` | No | Path to a CA bundle file for custom certificate authorities when using HTTPS. |

### Example `.env`

```dotenv
HVR_CONFIG=/opt/hvr/config
HVR_HUB=hvrhub
HVR_URI=https://hvr.example.com:4340/
HVR_USERNAME=admin
HVR_PASSWORD=secret
HVR_VERIFY_SSL=true
```

---

## Usage

```
python hvr_timestamp_refresh.py <mode> <channel> ["<userargs>"]
```

- `<mode>` — one of `refresh` or `dry_run` (required).
- `<channel>` — HVR channel name (required).
- `"<userargs>"` — optional string of flags, **enclosed in double quotes as a single argument**.

### Modes

| Mode | Description |
|---|---|
| `refresh` | Applies channel changes and creates the refresh job. |
| `dry_run` | Read-only simulation. Prints what would be changed/created without modifying anything. |

### Examples

**Dry run — inspect what would happen:**
```bash
python hvr_timestamp_refresh.py dry_run my_channel "-r src_loc -w tgt_loc -D MY_DSN -C load_ts -T schema.my_table"
```

**Incremental refresh:**
```bash
python hvr_timestamp_refresh.py refresh my_channel "-r src_loc -w tgt_loc -D MY_DSN -C load_ts -T schema.my_table"
```

**Incremental refresh with DDL check (recreate table if schema changed):**
```bash
python hvr_timestamp_refresh.py refresh my_channel "-r src_loc -w tgt_loc -D MY_DSN -C load_ts -T schema.my_table -d yes"
```

**Force-remove a stale block file left by a previous failure:**
```bash
python hvr_timestamp_refresh.py refresh my_channel "-r src_loc -w tgt_loc -D MY_DSN -C load_ts -T schema.my_table -b yes"
```

---

## Arguments Reference

All `userargs` are passed as a single quoted string after the channel name.

| Flag | Argument | Required | Default | Description |
|---|---|---|---|---|
| `-r` | `source_loc` | Yes | — | HVR source location name (read side). |
| `-w` | `target_loc` | Yes | — | HVR target location name (write side). |
| `-D` | `target_dsn` | Yes | — | ODBC DSN name for the target database. Used to query `MAX(ts_column)`. |
| `-C` | `ts_column` | Yes (with `-D`) | — | Column name used to determine the maximum replicated timestamp. |
| `-T` | `target_table` | Yes (with `-D`) | — | Fully qualified target table (`schema.table`) to query for max timestamp. |
| `-e` | `execution_context` | No | Auto-detected | Override execution context: `initial` or `incremental`. |
| `-d` | `ddl_check` | No | `no` | Check and reconcile table DDL during refresh. `yes` enables `recreate_if_mismatch`. |
| `-p` | `parallelism` | No | `6` | Number of parallel sessions for the refresh job. |
| `-t` | `trace_level` | No | `2` | Verbosity level. `0` = silent, `1` = options, `2` = full trace. |
| `-b` | `remove_block` | No | `no` | Set to `yes` to remove an existing block file before proceeding. |

---

## State and Block Files

State and block files are stored under:

```
$HVR_CONFIG/ts_refresh/<hub>/<channel>/
```

| File | Purpose |
|---|---|
| `<channel>.refr_state` | JSON file storing the last successful low-watermark: `{"low_watermark": "<ISO timestamp>", "retries": 0}` |
| `<channel>.block` | Presence of this file prevents a new refresh from starting. Created when a job is launched or fails; removed on successful completion. |

### Block file lifecycle

```
Job starts  →  block file created
Job PENDING →  block file retained (normal; job may still be in-flight)
Job FAILED  →  block file retained (manual investigation required, or use -b yes)
Job success →  block file removed, state file updated
```

---

## Cron Scheduling

Schedule the script with `cron` to run on a regular interval. Activate the virtual environment within the cron entry or use the full path to the venv interpreter.

```cron
# Run incremental refresh every 15 minutes
*/15 * * * * /path/to/hvr_timestamp_refresh/.venv/bin/python /path/to/hvr_timestamp_refresh/hvr_timestamp_refresh.py refresh my_channel "-r src_loc -w tgt_loc -D MY_DSN -C load_ts -T schema.my_table" >> /var/log/hvr_ts_refresh.log 2>&1
```

Key points:
- The block file mechanism prevents overlapping jobs if a previous run is still in flight.
- Ensure the cron user has read/write access to `$HVR_CONFIG/ts_refresh/`.
- Set `HVR_CONFIG`, `HVR_HUB`, `HVR_URI`, `HVR_USERNAME`, and `HVR_PASSWORD` in the cron environment or source a `.env` file via a wrapper script.

---

## Troubleshooting

| Symptom | Cause | Resolution |
|---|---|---|
| `HVR_CONFIG is not defined` | Environment variable missing | Set `HVR_CONFIG` in the shell or `.env`. |
| `Username must be provided` | `HVR_USERNAME` not set | Set `HVR_USERNAME` in the environment. |
| `a refresh is running; changes are not allowed` | Block file exists | Investigate the previous job. If it is resolved, rerun with `-b yes` to clear the block. |
| `Job is still running; a new job cannot yet be created` | The active refresh job has not finished | Wait for the job to complete or investigate in the HVR UI. |
| `Target table '...' not found` | ODBC `42S02` error | DDL check is auto-enabled; an initial load will be triggered. Verify DSN and table name. |
| SSL certificate errors | Self-signed or private CA | Set `REQUESTS_CA_BUNDLE` to the CA bundle path, or set `HVR_VERIFY_SSL=false` (not recommended for production). |
| ODBC connection failure | Driver not installed or DSN misconfigured | Verify the DSN with `isql -v MY_TARGET_DSN` and confirm the ODBC driver is installed. |
