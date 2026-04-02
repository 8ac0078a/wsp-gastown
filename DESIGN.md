# Windows Snapshot Proxy — Product Design Document

**Date:** 2026-04-02
**Status:** Draft
**Based on:** [Flexible Snapshot Proxy (experimental branch)](https://github.com/awslabs/flexible-snapshot-proxy/tree/experimental)

---

## 1. Overview

Windows Snapshot Proxy (WSP) extends the existing Flexible Snapshot Proxy (FSP) Python application with Windows Volume Shadow Copy Service (VSS) support. It adds two new commands to FSP:

- **`vss2s3`** — Creates a VSS shadow copy of a user-selected non-system Windows volume, reads it at the raw block level, and uploads it to S3 using FSP's existing compressed segment format.
- **`s3tovss`** — Downloads a previously uploaded VSS snapshot from S3 and restores it to a user-selected non-system Windows volume.

The application runs as a Windows command-line tool on Windows Server 2016+.

---

## 2. Goals & Non-Goals

### Goals
- Add Windows VSS snapshot-to-S3 and S3-to-volume restore commands to FSP
- Reuse FSP's existing S3 segment format (`{id}.{volsize}/{offset}.{checksum}.{length}.zstd`) for compatibility with `getfroms3`
- Support volumes up to 64 TB
- Support resumable uploads and downloads
- Support AWS CLI profiles and EC2 instance profile credentials
- Run on Windows Server 2016, 2019, 2022, and 2025

### Non-Goals
- System volume (boot volume) backup or restore
- Linux VSS equivalent (LVM snapshots)
- S3 multipart uploads
- GUI interface
- Incremental/differential VSS snapshots (full snapshot only)

---

## 3. Architecture

### 3.1 How It Fits Into FSP

WSP adds two new subcommands to FSP's existing `main.py` CLI parser, following the same pattern as `movetos3` and `getfroms3`. The new code lives in a new module `vss.py` alongside the existing `fsp.py`.

```
src/
├── main.py          # Modified — add vss2s3 and s3tovss subcommands
├── fsp.py           # Unchanged — existing FSP logic
├── vss.py           # New — VSS snapshot creation, raw read, restore
├── singleton.py     # Modified — add VSS-related global config
```

### 3.2 Data Flow — vss2s3 (Upload)

```
User selects volume (e.g. D:\)
        │
        ▼
 ┌──────────────┐
 │ Create VSS   │  Win32 API via wmi/subprocess (vssadmin or wbemtest)
 │ Shadow Copy  │
 └──────┬───────┘
        │  Shadow copy device path: \\?\GLOBALROOT\Device\HarddiskVolumeShadowCopyN
        ▼
 ┌──────────────┐
 │ Open shadow  │  CreateFile() on shadow copy device → raw block read
 │ copy as raw  │  Read in CHUNK_SIZE (512 KB) blocks
 │ block device │
 └──────┬───────┘
        │
        ▼
 ┌──────────────┐
 │ Segment &    │  Reuse FSP's chunk_and_align() logic
 │ compress     │  zstandard compression per segment
 │ (zstd)       │  SHA-256 checksum per segment
 └──────┬───────┘
        │
        ▼
 ┌──────────────┐
 │ Upload to S3 │  Parallel upload via joblib
 │ (segments)   │  FSP segment naming convention
 └──────┬───────┘
        │
        ▼
 ┌──────────────┐
 │ Write resume │  JSON manifest tracking uploaded segments
 │ manifest     │
 └──────┬───────┘
        │
        ▼
  Prompt user: delete VSS shadow copy? (y/n)
```

### 3.3 Data Flow — s3tovss (Restore)

```
User selects target volume (e.g. E:\) and S3 snapshot prefix
        │
        ▼
 ┌──────────────┐
 │ List S3      │  Enumerate segments under snapshot prefix
 │ objects      │
 └──────┬───────┘
        │
        ▼
 ┌──────────────┐
 │ Download &   │  Parallel download via joblib
 │ decompress   │  Verify SHA-256 checksums
 │ segments     │
 └──────┬───────┘
        │
        ▼
 ┌──────────────┐
 │ Write raw    │  Open target volume as raw device
 │ blocks to    │  Write at correct offsets
 │ volume       │  (\\.\D: or \\.\PhysicalDriveN)
 └──────┬───────┘
        │
        ▼
  Done — volume restored
```

---

## 4. Detailed Design

### 4.1 VSS Shadow Copy Creation

The application uses `vssadmin` (available on all Windows Server 2016+ systems) to create and manage shadow copies:

```
vssadmin create shadow /for=D:
```

Parsing the output yields the Shadow Copy ID and Shadow Copy Volume Name (device path). The device path is of the form:

```
\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopyN
```

This path can be opened with `CreateFile()` (via Python's `ctypes` or `win32file`) for raw block-level reads.

**Volume enumeration:** The application lists available non-system volumes using WMI (`Win32_Volume`) or `Get-Volume` via subprocess, filtering out:
- The system volume (where Windows is installed)
- Recovery partitions
- Volumes with no drive letter

The user is presented with a numbered list and prompted to select one.

### 4.2 Raw Block Reading on Windows

Windows raw volume access requires:
1. Opening the shadow copy device path with `CreateFile()` using `GENERIC_READ`, `FILE_SHARE_READ`, and `FILE_FLAG_NO_BUFFERING`
2. Reading in sector-aligned chunks (FSP's 512 KB `CHUNK_SIZE` is already sector-aligned)
3. Running as Administrator (required for raw device access)

Python implementation uses `ctypes` to call Win32 APIs directly, avoiding a dependency on `pywin32`:

```python
import ctypes
from ctypes import wintypes

kernel32 = ctypes.windll.kernel32

handle = kernel32.CreateFileW(
    shadow_device_path,
    0x80000000,  # GENERIC_READ
    0x00000001,  # FILE_SHARE_READ
    None,
    3,           # OPEN_EXISTING
    0x20000000,  # FILE_FLAG_NO_BUFFERING
    None
)
```

### 4.3 S3 Segment Format (Reused from FSP)

Each segment is stored as an S3 object with the key format:

```
{snapshot_id}.{volume_size_gb}/{offset}.{checksum}.{block_count}.zstd
```

Where:
- `snapshot_id` — A generated unique identifier (e.g., `vss-{timestamp}-{volume_letter}`)
- `volume_size_gb` — Volume size in GiB
- `offset` — Starting block index of the segment
- `checksum` — URL-safe base64-encoded SHA-256 of the uncompressed segment data
- `block_count` — Number of 512 KB blocks in the segment
- `.zstd` — Indicates zstandard compression

This is identical to FSP's `movetos3` format, meaning snapshots uploaded by WSP can be restored using FSP's `getfroms3` command to an EBS snapshot, and vice versa.

### 4.4 Parallelism

Reuses FSP's parallelism model:
- Block map is split into `NUM_JOBS` segments using `numpy.array_split()`
- Each segment is processed in a `joblib.Parallel` worker
- Each worker spawns its own `boto3` S3 client
- `NUM_JOBS` defaults to 16 for local operations (same as FSP single-region default)

### 4.5 Resume Support

A JSON manifest file is written to the local filesystem during upload/download:

```json
{
  "operation": "vss2s3",
  "snapshot_id": "vss-20260402-D",
  "volume": "D:",
  "volume_size_gb": 500,
  "s3_bucket": "my-backup-bucket",
  "total_segments": 1024,
  "completed_segments": [0, 64, 128, ...],
  "timestamp": "2026-04-02T10:00:00Z"
}
```

Location: `%TEMP%\wsp_resume_{snapshot_id}.json`

On startup, if a resume manifest exists for the same volume + bucket combination, the user is prompted:

```
Resume previous upload of D: to s3://my-backup-bucket? (y/n):
```

If yes, already-uploaded segments are skipped. The manifest is deleted on successful completion.

### 4.6 Credential Handling

Credentials are resolved in this order (standard boto3 chain):
1. EC2 instance profile / IAM role (if running on EC2)
2. AWS CLI profile specified via `--profile` flag
3. Default AWS CLI profile (`~/.aws/credentials`)

No interactive credential prompting. The user configures credentials before running the tool using `aws configure` or instance metadata.

The `--endpoint_url` flag (already in FSP) is supported for custom S3 endpoints (e.g., S3-compatible storage, Snowball Edge).

### 4.7 System Volume Exclusion

System volume detection uses:

```python
os.environ['SystemDrive']  # Typically "C:"
```

Both `vss2s3` and `s3tovss` refuse to operate on the system volume and exit with an error message.

### 4.8 VSS Snapshot Lifecycle

After a successful `vss2s3` upload:

```
Upload complete. VSS shadow copy {shadow_id} still exists.
Delete the shadow copy? (y/n):
```

- `y` → runs `vssadmin delete shadows /shadow={shadow_id} /quiet`
- `n` → prints the shadow copy ID for manual cleanup

If the upload fails, the shadow copy is always preserved (user may want to retry).

---

## 5. CLI Interface

### 5.1 New Commands

#### vss2s3

```
python src/main.py vss2s3 <s3Bucket> [options]

Positional arguments:
  s3Bucket              Target S3 bucket name

Options:
  --volume LETTER       Volume drive letter (e.g. D). If omitted, user is prompted.
  -d, --destination_region REGION
                        AWS region where the S3 bucket exists (default: origin region)
  -e, --endpoint_url URL
                        Custom S3 endpoint URL
  -p, --profile NAME    AWS CLI profile name (default: "default")
  --resume              Resume a previously interrupted upload
  --log FILE            Write detailed log to FILE
```

#### s3tovss

```
python src/main.py s3tovss <snapshot_prefix> <s3Bucket> [options]

Positional arguments:
  snapshot_prefix       S3 prefix of the snapshot to restore (e.g. vss-20260402-D.500)
  s3Bucket              Source S3 bucket name

Options:
  --volume LETTER       Target volume drive letter (e.g. E). If omitted, user is prompted.
  -d, --destination_region REGION
                        AWS region where the S3 bucket exists (default: origin region)
  -e, --endpoint_url URL
                        Custom S3 endpoint URL
  -p, --profile NAME    AWS CLI profile name (default: "default")
  --resume              Resume a previously interrupted restore
  --log FILE            Write detailed log to FILE
```

### 5.2 Example Usage

**Backup volume D: to S3:**
```
python src/main.py vss2s3 my-backup-bucket --volume D
```

**Interactive volume selection:**
```
python src/main.py vss2s3 my-backup-bucket

Available volumes (excluding system volume C:):
  [1] D:  500 GB  NTFS  "Data"
  [2] E:  1000 GB NTFS  "Logs"

Select volume [1-2]: 1
Creating VSS shadow copy of D:...
Shadow copy created: {AB12CD34-...}
Uploading to s3://my-backup-bucket/vss-20260402-D.500/...
Progress: 45% (225 GB / 500 GB) | Speed: 450 MB/s | ETA: 10m 12s
...
Upload complete. VSS shadow copy {AB12CD34-...} still exists.
Delete the shadow copy? (y/n): y
Shadow copy deleted.
```

**Restore from S3 to volume E::**
```
python src/main.py s3tovss vss-20260402-D.500 my-backup-bucket --volume E
```

---

## 6. Logging

- **Default (console):** Progress line showing percentage, speed, and ETA. Errors printed to stderr.
- **`--log FILE`:** Appends detailed per-segment logging (segment offset, checksum, size, duration, retries) to the specified file. Format matches FSP's verbosity level 2 (`-vv`).

---

## 7. Error Handling

| Error | Behavior |
|---|---|
| Not running as Administrator | Exit with message: "This command requires Administrator privileges." |
| System volume selected | Exit with message: "Cannot operate on system volume {drive}." |
| VSS shadow copy creation fails | Exit with vssadmin error output |
| S3 bucket inaccessible | Exit with boto3 error (reuses FSP's `validate_s3_bucket()`) |
| Network interruption during upload | Retry individual segment (FSP retry logic). Write resume manifest. |
| Checksum mismatch on download | Retry segment indefinitely (matches FSP behavior) |
| Volume too small for restore | Exit with message showing required vs. available size |
| Credential error | Exit with message: "Unable to locate credentials. Run 'aws configure'." (matches FSP) |

---

## 8. IAM Permissions

The existing FSP IAM policy is sufficient. The S3 permissions needed:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketAcl",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::my-backup-bucket",
                "arn:aws:s3:::my-backup-bucket/*"
            ]
        }
    ]
}
```

No EBS Direct API permissions are needed for the VSS commands (those are local operations).

---

## 9. Dependencies

Existing FSP dependencies (no new pip packages required):

| Package | Purpose |
|---|---|
| boto3 | AWS SDK — S3 operations |
| numpy | Array splitting for parallelism |
| joblib | Parallel execution |
| zstandard | Compression |

Windows-specific (no pip install — uses Python stdlib + ctypes):

| Component | Purpose |
|---|---|
| ctypes (stdlib) | Win32 API calls for raw device access |
| subprocess (stdlib) | vssadmin commands, volume enumeration |
| json (stdlib) | Resume manifest |

---

## 10. Platform Compatibility

| Component | Windows | Linux/macOS |
|---|---|---|
| Existing FSP commands | ✅ (unchanged) | ✅ (unchanged) |
| `vss2s3` | ✅ Windows Server 2016+ | ❌ Exits with "Windows only" error |
| `s3tovss` | ✅ Windows Server 2016+ | ❌ Exits with "Windows only" error |

The new commands check `platform.system() == "Windows"` at entry and exit gracefully on other platforms. All existing FSP functionality remains unaffected.

---

## 11. Security Considerations

- **Administrator required:** Raw volume access and VSS operations require elevated privileges. The application checks and exits early if not running as Administrator.
- **No credentials stored:** Relies entirely on the boto3 credential chain. No credentials are written to disk or logs.
- **Shadow copy access:** The VSS shadow copy is read-only. The original volume is not modified during backup.
- **Restore is destructive:** `s3tovss` overwrites the target volume. The application prints a confirmation prompt before proceeding:
  ```
  WARNING: This will overwrite all data on E: (1000 GB). Continue? (y/n):
  ```

---

## 12. Testing Strategy

| Test | Method |
|---|---|
| VSS shadow copy create/delete | Manual on Windows Server 2016 VM |
| Raw block read from shadow copy | Unit test with mock device handle |
| S3 segment format compatibility | Upload via `vss2s3`, restore via FSP `getfroms3` to EBS snapshot |
| Resume after interruption | Kill process mid-upload, verify resume skips completed segments |
| System volume exclusion | Attempt `--volume C` on a system where C: is the boot drive |
| Non-Windows platform guard | Run `vss2s3` on Linux, verify clean error message |
| Large volume (>5 GB segments) | Verify no single S3 PutObject exceeds 5 GB (segment size is controlled by `chunk_and_align` offset parameter — default 64 blocks × 512 KB = 32 MB) |

---

## 13. Future Considerations

- **Incremental VSS snapshots:** Use VSS differential shadow copies to upload only changed blocks (would require a local block-change tracking mechanism).
- **Scheduled backups:** Wrap WSP in Windows Task Scheduler for automated periodic backups.
- **System volume support:** Could be added later with bare-metal restore tooling (boot from WinPE, restore system volume).
- **VHD/VHDX export:** Option to store as a mountable disk image instead of FSP segment format.
