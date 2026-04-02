"""
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""

# WSP — Windows Snapshot Proxy
# VSS integration: shadow copy lifecycle, raw block read/write, S3 transfer.

import ctypes
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
import time
from base64 import urlsafe_b64encode
from ctypes import wintypes  # noqa: F401 — used by Win32 API callers
from datetime import datetime

CHUNK_SIZE = 1024 * 512  # 512 KB — sector-aligned, matches FSP


def _require_windows():
    """Exit with a clean message if not running on Windows."""
    if platform.system() != "Windows":
        print("Error: This command is only supported on Windows.", file=sys.stderr)
        sys.exit(1)


def _require_admin():
    """Exit with a clean message if not running as Administrator."""
    try:
        is_admin = ctypes.windll.shell32.IsUserAnAdmin()
    except AttributeError:
        is_admin = False
    if not is_admin:
        print("Error: This command requires Administrator privileges.", file=sys.stderr)
        sys.exit(1)


def list_non_system_volumes():
    """Return a list of non-system, non-recovery volumes available for backup/restore.

    Each entry is a dict with keys: letter, size_gb, filesystem, label.
    Excludes the system volume (os.environ['SystemDrive']) and volumes without
    a drive letter.

    Returns:
        list[dict]: Available volumes.
    """
    system_drive = os.environ.get("SystemDrive", "C:").rstrip(":").upper()

    ps_script = (
        "Get-Volume | "
        "Where-Object { $_.DriveLetter -ne $null -and $_.DriveType -eq 'Fixed' } | "
        "Select-Object DriveLetter, Size, FileSystemType, FileSystemLabel | "
        "ConvertTo-Json"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_script],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print("Error: Failed to enumerate volumes.", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    raw_output = result.stdout.strip()
    if not raw_output:
        return []

    try:
        raw = json.loads(raw_output)
    except json.JSONDecodeError:
        print("Error: Could not parse volume information.", file=sys.stderr)
        sys.exit(1)

    # Normalize to list if PowerShell returned a single object
    if isinstance(raw, dict):
        raw = [raw]

    volumes = []
    for v in raw:
        letter = str(v.get("DriveLetter") or "").upper()
        if not letter or letter == system_drive:
            continue
        size_bytes = v.get("Size") or 0
        size_gb = math.ceil(size_bytes / (1024 ** 3))
        fs = v.get("FileSystemType") or ""
        label = v.get("FileSystemLabel") or ""
        volumes.append({
            "letter": letter,
            "size_gb": size_gb,
            "filesystem": fs,
            "label": label,
        })

    return volumes


def _get_volume_size_bytes(volume_letter):
    """Return the total size in bytes for the given drive letter.

    Args:
        volume_letter (str): Single drive letter, e.g. ``"D"``.

    Returns:
        int: Volume size in bytes.
    """
    letter = volume_letter.upper().rstrip(":")
    ps_script = f"(Get-Volume -DriveLetter {letter}).Size"
    result = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_script],
        capture_output=True, text=True
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"Could not get size for volume {letter}:")
    return int(result.stdout.strip())


def create_vss_shadow(volume_letter):
    """Create a VSS shadow copy for the given volume.

    Runs ``vssadmin create shadow /for=<LETTER>:`` and parses the output to
    extract the Shadow Copy ID and Shadow Copy Volume (device path).

    Args:
        volume_letter (str): Single drive letter, e.g. ``"D"``.

    Returns:
        tuple[str, str]: ``(shadow_id, shadow_device_path)``
            - ``shadow_id`` — GUID string, e.g. ``"{AB12CD34-...}"``
            - ``shadow_device_path`` — raw device path, e.g.
              ``"\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopyN"``
    """
    letter = volume_letter.upper().rstrip(":")
    result = subprocess.run(
        ["vssadmin", "create", "shadow", f"/for={letter}:"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print("Error: Failed to create VSS shadow copy.", file=sys.stderr)
        print(result.stdout, file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    shadow_id = None
    shadow_device = None
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("Shadow Copy ID:"):
            shadow_id = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Shadow Copy Volume Name:"):
            shadow_device = stripped.split(":", 1)[1].strip()

    if shadow_id is None or shadow_device is None:
        print("Error: Could not parse vssadmin output.", file=sys.stderr)
        print(result.stdout, file=sys.stderr)
        sys.exit(1)

    return shadow_id, shadow_device


def delete_vss_shadow(shadow_id):
    """Delete a VSS shadow copy by its ID.

    Runs ``vssadmin delete shadows /shadow=<shadow_id> /quiet``.

    Args:
        shadow_id (str): GUID string returned by :func:`create_vss_shadow`.
    """
    result = subprocess.run(
        ["vssadmin", "delete", "shadows", f"/shadow={shadow_id}", "/quiet"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Warning: Could not delete shadow copy {shadow_id}.", file=sys.stderr)
        print(result.stdout, file=sys.stderr)


def open_shadow_device(shadow_device_path):
    """Open a VSS shadow copy device for raw block reading.

    Uses ``CreateFileW`` via ``ctypes`` with ``GENERIC_READ``,
    ``FILE_SHARE_READ``, and ``FILE_FLAG_NO_BUFFERING``.  Must be called
    with Administrator privileges.

    Args:
        shadow_device_path (str): Device path from :func:`create_vss_shadow`.

    Returns:
        int: Win32 HANDLE value (opaque integer).
    """
    GENERIC_READ = 0x80000000
    FILE_SHARE_READ = 0x00000001
    OPEN_EXISTING = 3
    FILE_FLAG_NO_BUFFERING = 0x20000000

    kernel32 = ctypes.windll.kernel32
    handle = kernel32.CreateFileW(
        shadow_device_path,
        GENERIC_READ,
        FILE_SHARE_READ,
        None,
        OPEN_EXISTING,
        FILE_FLAG_NO_BUFFERING,
        None,
    )
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    if handle == INVALID_HANDLE_VALUE or handle == 0:
        err = kernel32.GetLastError()
        print(
            f"Error: Failed to open shadow device '{shadow_device_path}' "
            f"(Win32 error {err}).",
            file=sys.stderr
        )
        sys.exit(1)
    return handle


def read_raw_blocks(handle, offset_bytes, length_bytes):
    """Read raw bytes from an open device handle.

    Args:
        handle (int): Win32 HANDLE from :func:`open_shadow_device`.
        offset_bytes (int): Byte offset to seek to before reading.
        length_bytes (int): Number of bytes to read (must be sector-aligned).

    Returns:
        bytes: Raw block data.
    """
    kernel32 = ctypes.windll.kernel32

    new_pos = ctypes.c_int64(0)
    ok = kernel32.SetFilePointerEx(
        handle,
        ctypes.c_int64(offset_bytes),
        ctypes.byref(new_pos),
        0,  # FILE_BEGIN
    )
    if not ok:
        err = kernel32.GetLastError()
        raise OSError(
            f"SetFilePointerEx failed at offset {offset_bytes} (Win32 error {err})"
        )

    buf = ctypes.create_string_buffer(length_bytes)
    bytes_read = ctypes.wintypes.DWORD(0)  # DWORD = uint32, as expected by ReadFile
    ok = kernel32.ReadFile(
        handle,
        buf,
        length_bytes,
        ctypes.byref(bytes_read),
        None,
    )
    if not ok:
        err = kernel32.GetLastError()
        raise OSError(
            f"ReadFile failed at offset {offset_bytes} (Win32 error {err})"
        )

    return bytes(buf.raw[:bytes_read.value])


def close_device_handle(handle):
    """Close a Win32 device handle.

    Args:
        handle (int): Win32 HANDLE from :func:`open_shadow_device`.
    """
    ctypes.windll.kernel32.CloseHandle(handle)


def open_volume_for_write(volume_letter):
    """Open a Windows volume as a raw device for writing.

    Uses ``CreateFileW`` via ``ctypes`` with ``GENERIC_WRITE``,
    ``FILE_SHARE_READ | FILE_SHARE_WRITE``, ``FILE_FLAG_NO_BUFFERING``, and
    ``FILE_FLAG_WRITE_THROUGH``.  Must be called with Administrator privileges.

    Args:
        volume_letter (str): Single drive letter, e.g. ``"E"``.

    Returns:
        int: Win32 HANDLE value (opaque integer).
    """
    GENERIC_WRITE = 0x40000000
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    OPEN_EXISTING = 3
    FILE_FLAG_NO_BUFFERING = 0x20000000
    FILE_FLAG_WRITE_THROUGH = 0x80000000

    letter = volume_letter.upper().rstrip(":")
    path = f"\\\\.\\{letter}:"

    kernel32 = ctypes.windll.kernel32
    handle = kernel32.CreateFileW(
        path,
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        None,
        OPEN_EXISTING,
        FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH,
        None,
    )
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    if handle == INVALID_HANDLE_VALUE or handle == 0:
        err = kernel32.GetLastError()
        print(
            f"Error: Failed to open volume '{letter}:' for writing "
            f"(Win32 error {err}).",
            file=sys.stderr
        )
        sys.exit(1)
    return handle


def write_raw_blocks(handle, offset_bytes, data):
    """Write raw bytes to an open device handle at a specific offset.

    Args:
        handle (int): Win32 HANDLE from :func:`open_volume_for_write`.
        offset_bytes (int): Byte offset to seek to before writing.
        data (bytes): Data to write (must be sector-aligned in length).

    Returns:
        int: Number of bytes written.
    """
    kernel32 = ctypes.windll.kernel32

    new_pos = ctypes.c_int64(0)
    ok = kernel32.SetFilePointerEx(
        handle,
        ctypes.c_int64(offset_bytes),
        ctypes.byref(new_pos),
        0,  # FILE_BEGIN
    )
    if not ok:
        err = kernel32.GetLastError()
        raise OSError(
            f"SetFilePointerEx failed at offset {offset_bytes} (Win32 error {err})"
        )

    buf = ctypes.create_string_buffer(data)
    bytes_written = ctypes.wintypes.DWORD(0)
    ok = kernel32.WriteFile(
        handle,
        buf,
        len(data),
        ctypes.byref(bytes_written),
        None,
    )
    if not ok:
        err = kernel32.GetLastError()
        raise OSError(
            f"WriteFile failed at offset {offset_bytes} (Win32 error {err})"
        )
    return bytes_written.value


# ---------------------------------------------------------------------------
# Resume manifest helpers
# ---------------------------------------------------------------------------

def _resume_manifest_path(snapshot_id):
    """Return the path for the resume manifest file."""
    temp = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
    return os.path.join(temp, f"wsp_resume_{snapshot_id}.json")


def _find_resume_manifest(volume_letter, s3_bucket):
    """Search for an existing resume manifest matching the given volume and bucket.

    Returns:
        dict | None: Manifest data with a ``manifest_path`` key added, or None.
    """
    temp = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
    letter = volume_letter.upper().rstrip(":")
    try:
        entries = os.listdir(temp)
    except OSError:
        return None

    for fname in entries:
        if fname.startswith("wsp_resume_") and fname.endswith(".json"):
            fpath = os.path.join(temp, fname)
            try:
                with open(fpath) as f:
                    data = json.load(f)
                vol_match = data.get("volume", "").upper().rstrip(":") == letter
                bucket_match = data.get("s3_bucket") == s3_bucket
                op_match = data.get("operation") == "vss2s3"
                if vol_match and bucket_match and op_match:
                    data["manifest_path"] = fpath
                    return data
            except (json.JSONDecodeError, KeyError, OSError):
                continue
    return None


def _write_resume_manifest(manifest_path, snapshot_id, volume_letter,
                            s3_bucket, total_segments, completed_offsets):
    """Write (or overwrite) the resume manifest to disk."""
    data = {
        "operation": "vss2s3",
        "snapshot_id": snapshot_id,
        "volume": f"{volume_letter.upper().rstrip(':')}:",
        "s3_bucket": s3_bucket,
        "total_segments": total_segments,
        "completed_segments": sorted(completed_offsets),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    with open(manifest_path, "w") as f:
        json.dump(data, f, indent=2)


def _find_restore_resume_manifest(snapshot_prefix, s3_bucket):
    """Search for an existing restore resume manifest matching the given prefix and bucket.

    Returns:
        dict | None: Manifest data with a ``manifest_path`` key added, or None.
    """
    temp = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
    try:
        entries = os.listdir(temp)
    except OSError:
        return None

    for fname in entries:
        if fname.startswith("wsp_resume_") and fname.endswith(".json"):
            fpath = os.path.join(temp, fname)
            try:
                with open(fpath) as f:
                    data = json.load(f)
                prefix_match = data.get("snapshot_prefix") == snapshot_prefix
                bucket_match = data.get("s3_bucket") == s3_bucket
                op_match = data.get("operation") == "s3tovss"
                if prefix_match and bucket_match and op_match:
                    data["manifest_path"] = fpath
                    return data
            except (json.JSONDecodeError, KeyError, OSError):
                continue
    return None


def _write_restore_resume_manifest(manifest_path, snapshot_prefix, volume_letter,
                                    s3_bucket, total_segments, completed_offsets):
    """Write (or overwrite) the restore resume manifest to disk."""
    data = {
        "operation": "s3tovss",
        "snapshot_prefix": snapshot_prefix,
        "volume": f"{volume_letter.upper().rstrip(':')}:",
        "s3_bucket": s3_bucket,
        "total_segments": total_segments,
        "completed_segments": sorted(completed_offsets),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    with open(manifest_path, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Per-segment restore worker (runs in joblib subprocess)
# ---------------------------------------------------------------------------

def _restore_s3_segment(s3_key, s3_bucket, volume_letter, region,
                         endpoint_url, profile, log_file):
    """Download one segment from S3, decompress, verify checksum, and write to volume.

    This function is called in a joblib parallel worker.  Each invocation
    opens its own boto3 S3 client and Win32 volume handle so it is safe to
    run concurrently.

    S3 key format: ``{snapshot_prefix}/{offset}.{checksum}.{block_count}.zstd``

    Args:
        s3_key (str): Full S3 object key.
        s3_bucket (str): Source S3 bucket name.
        volume_letter (str): Target drive letter (e.g. ``"E"``).
        region (str): AWS region for the S3 bucket.
        endpoint_url (str | None): Custom S3 endpoint URL.
        profile (str): AWS CLI profile name.
        log_file (str | None): Path for per-segment log appends.

    Returns:
        int: Starting block index (offset) of this segment, for manifest tracking.
    """
    import zstandard
    import boto3

    # Parse key: {prefix}/{offset}.{checksum}.{block_count}.zstd
    key_name = s3_key.split("/", 1)[1] if "/" in s3_key else s3_key
    parts = key_name.split(".")
    offset = int(parts[0])
    expected_checksum = parts[1]
    block_count = int(parts[2])

    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3", region_name=region, endpoint_url=endpoint_url)
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    compressed = response["Body"].read()

    dctx = zstandard.ZstdDecompressor()
    raw = dctx.decompress(compressed)

    h = hashlib.sha256()
    h.update(raw)
    actual_checksum = urlsafe_b64encode(h.digest()).decode()
    if actual_checksum != expected_checksum:
        raise ValueError(
            f"Checksum mismatch for segment at offset {offset}: "
            f"expected {expected_checksum}, got {actual_checksum}"
        )

    handle = open_volume_for_write(volume_letter)
    try:
        write_raw_blocks(handle, offset * CHUNK_SIZE, raw)
    finally:
        close_device_handle(handle)

    if log_file:
        try:
            with open(log_file, "a") as lf:
                lf.write(
                    f"{datetime.utcnow().isoformat()}Z  "
                    f"offset={offset}  blocks={block_count}  "
                    f"compressed={len(compressed)}  raw={len(raw)}  "
                    f"checksum={actual_checksum}  key={s3_key}\n"
                )
        except OSError:
            pass

    return offset


# ---------------------------------------------------------------------------
# Per-segment upload worker (runs in joblib subprocess)
# ---------------------------------------------------------------------------

def _upload_vss_segment(shadow_device_path, segment, snapshot_id, vol_size_gb,
                         s3_bucket, region, endpoint_url, profile, log_file):
    """Read one segment from the shadow device and upload it to S3.

    This function is called in a joblib parallel worker.  Each invocation
    opens its own Win32 handle and boto3 S3 client so it is safe to run
    concurrently.

    Args:
        shadow_device_path (str): VSS shadow device path.
        segment (list[dict]): Block list with ``BlockIndex`` keys.
        snapshot_id (str): Snapshot identifier used in the S3 key.
        vol_size_gb (int): Volume size in GiB used in the S3 key.
        s3_bucket (str): Target S3 bucket name.
        region (str): AWS region for the S3 bucket.
        endpoint_url (str | None): Custom S3 endpoint URL.
        profile (str): AWS CLI profile name.
        log_file (str | None): Path for per-segment log appends.

    Returns:
        int: Starting block index (offset) of this segment, for manifest tracking.
    """
    import zstandard
    import boto3

    offset = segment[0]["BlockIndex"]

    handle = open_shadow_device(shadow_device_path)
    try:
        data = bytearray()
        for block in segment:
            chunk = read_raw_blocks(
                handle, block["BlockIndex"] * CHUNK_SIZE, CHUNK_SIZE
            )
            data += chunk
    finally:
        close_device_handle(handle)

    data = bytes(data)
    h = hashlib.sha256()
    h.update(data)
    checksum = urlsafe_b64encode(h.digest()).decode()
    block_count = len(segment)

    compressed = zstandard.compress(data, 1)
    s3_key = f"{snapshot_id}.{vol_size_gb}/{offset}.{checksum}.{block_count}.zstd"

    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3", region_name=region, endpoint_url=endpoint_url)

    retry_count = 0
    upload_start = time.perf_counter()
    while True:
        try:
            s3.put_object(Body=compressed, Bucket=s3_bucket, Key=s3_key)
            break
        except Exception:
            retry_count += 1
            time.sleep(min(2 ** retry_count, 30))
    upload_duration = time.perf_counter() - upload_start

    if log_file:
        try:
            with open(log_file, "a") as lf:
                lf.write(
                    f"{datetime.utcnow().isoformat()}Z  "
                    f"offset={offset}  blocks={block_count}  "
                    f"raw={len(data)}  compressed={len(compressed)}  "
                    f"checksum={checksum}  duration={upload_duration:.3f}s  "
                    f"retries={retry_count}  key={s3_key}\n"
                )
        except OSError:
            pass

    return offset


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def vss2s3(s3_bucket, volume_letter=None, destination_region=None,
           endpoint_url=None, profile="default", resume=False, log_file=None):
    """Upload a VSS shadow copy of a Windows volume to S3.

    Workflow:
    1. Check platform (Windows) and privileges (Administrator).
    2. Enumerate and prompt for volume selection if ``volume_letter`` is None.
    3. Create VSS shadow copy.
    4. Read raw blocks from shadow copy device.
    5. Segment, compress (zstd), checksum (SHA-256), and upload to S3.
    6. Write/update resume manifest.
    7. Prompt user to delete shadow copy.

    Args:
        s3_bucket (str): Target S3 bucket name.
        volume_letter (str | None): Drive letter (e.g. ``"D"``).  If None,
            the user is prompted interactively.
        destination_region (str | None): AWS region for the S3 bucket.
        endpoint_url (str | None): Custom S3 endpoint URL.
        profile (str): AWS CLI profile name.
        resume (bool): Resume a previously interrupted upload if a manifest
            exists.
        log_file (str | None): Path for detailed per-segment log output.
    """
    _require_windows()
    _require_admin()

    from fsp import chunk_and_align
    from joblib import Parallel, delayed
    from singleton import SingletonClass
    singleton = SingletonClass()

    s3_region = (
        destination_region
        or singleton.AWS_DEST_REGION
        or singleton.AWS_ORIGIN_REGION
        or "us-east-1"
    )
    num_jobs = singleton.NUM_JOBS or 16

    # ------------------------------------------------------------------
    # Volume selection
    # ------------------------------------------------------------------
    system_drive = os.environ.get("SystemDrive", "C:").rstrip(":").upper()

    if volume_letter is None:
        volumes = list_non_system_volumes()
        if not volumes:
            print("Error: No eligible volumes found.", file=sys.stderr)
            sys.exit(1)
        print(f"\nAvailable volumes (excluding system volume {system_drive}:):")
        for i, v in enumerate(volumes, 1):
            print(
                f"  [{i}] {v['letter']}:  {v['size_gb']} GB  "
                f"{v['filesystem']}  \"{v['label']}\""
            )
        while True:
            try:
                raw = input(f"\nSelect volume [1-{len(volumes)}]: ").strip()
                choice = int(raw) - 1
                if 0 <= choice < len(volumes):
                    volume_letter = volumes[choice]["letter"]
                    break
                print(f"Please enter a number between 1 and {len(volumes)}.")
            except ValueError:
                print("Invalid input.")
    else:
        volume_letter = volume_letter.upper().rstrip(":")
        if volume_letter == system_drive:
            print(
                f"Error: Cannot operate on system volume {volume_letter}:.",
                file=sys.stderr
            )
            sys.exit(1)

    # ------------------------------------------------------------------
    # Volume size
    # ------------------------------------------------------------------
    try:
        vol_size_bytes = _get_volume_size_bytes(volume_letter)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    vol_size_gb = math.ceil(vol_size_bytes / (1024 ** 3))
    total_chunks = vol_size_bytes // CHUNK_SIZE

    # ------------------------------------------------------------------
    # Resume manifest
    # ------------------------------------------------------------------
    snapshot_id = (
        f"vss-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{volume_letter}"
    )
    completed_offsets = set()

    if resume:
        existing = _find_resume_manifest(volume_letter, s3_bucket)
        if existing:
            choice = input(
                f"Resume previous upload of {volume_letter}: "
                f"to s3://{s3_bucket}? (y/n): "
            ).strip()
            if choice.lower() == "y":
                snapshot_id = existing["snapshot_id"]
                completed_offsets = set(existing.get("completed_segments", []))
                print(
                    f"Resuming snapshot {snapshot_id}, "
                    f"{len(completed_offsets)} segments already uploaded."
                )

    manifest_path = _resume_manifest_path(snapshot_id)

    # ------------------------------------------------------------------
    # Build segment list
    # ------------------------------------------------------------------
    blocks = [{"BlockIndex": i} for i in range(total_chunks)]
    all_segments = chunk_and_align(blocks, 1, 64)
    total_segments = len(all_segments)
    pending_segments = [
        seg for seg in all_segments
        if seg[0]["BlockIndex"] not in completed_offsets
    ]

    _write_resume_manifest(
        manifest_path, snapshot_id, volume_letter,
        s3_bucket, total_segments, completed_offsets
    )

    # ------------------------------------------------------------------
    # Create VSS shadow copy
    # ------------------------------------------------------------------
    print(f"Creating VSS shadow copy of {volume_letter}:...")
    shadow_id, shadow_device_path = create_vss_shadow(volume_letter)
    singleton.VSS_SHADOW_ID = shadow_id
    singleton.VSS_SHADOW_DEVICE_PATH = shadow_device_path
    print(f"Shadow copy created: {shadow_id}")
    print(f"Uploading to s3://{s3_bucket}/{snapshot_id}.{vol_size_gb}/...")

    # ------------------------------------------------------------------
    # Parallel upload — process in batches for progress display
    # ------------------------------------------------------------------
    start_time = time.perf_counter()
    batch_size = max(num_jobs * 4, 64)

    try:
        with Parallel(n_jobs=num_jobs) as parallel:
            for batch_start in range(0, len(pending_segments), batch_size):
                batch = pending_segments[batch_start: batch_start + batch_size]
                results = parallel(
                    delayed(_upload_vss_segment)(
                        shadow_device_path, seg, snapshot_id, vol_size_gb,
                        s3_bucket, s3_region, endpoint_url, profile, log_file
                    )
                    for seg in batch
                )
                for offset in results:
                    completed_offsets.add(offset)

                _write_resume_manifest(
                    manifest_path, snapshot_id, volume_letter,
                    s3_bucket, total_segments, completed_offsets
                )

                done = len(completed_offsets)
                elapsed = time.perf_counter() - start_time
                bytes_done = done * 64 * CHUNK_SIZE
                speed = bytes_done / elapsed if elapsed > 0 else 0
                remaining = total_segments - done
                eta_s = (
                    remaining * 64 * CHUNK_SIZE / speed
                    if speed > 0 else 0
                )
                print(
                    f"\rProgress: {100.0 * done / total_segments:.0f}% "
                    f"({bytes_done // (1024 ** 3)} GB / {vol_size_gb} GB) | "
                    f"Speed: {speed / (1024 ** 2):.0f} MB/s | "
                    f"ETA: {int(eta_s // 60)}m {int(eta_s % 60)}s",
                    end="", flush=True
                )

    except Exception as exc:
        print(f"\nError during upload: {exc}", file=sys.stderr)
        print(
            f"Upload interrupted. Resume manifest saved to: {manifest_path}",
            file=sys.stderr
        )
        print(f"Shadow copy preserved: {shadow_id}", file=sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Success
    # ------------------------------------------------------------------
    elapsed = time.perf_counter() - start_time
    total_bytes = total_chunks * CHUNK_SIZE
    speed = total_bytes / elapsed if elapsed > 0 else 0
    print(
        f"\nUpload complete. "
        f"Took {elapsed:.1f}s at {speed / (1024 ** 2):.1f} MB/s."
    )

    try:
        os.remove(manifest_path)
    except OSError:
        pass

    # ------------------------------------------------------------------
    # Prompt to delete shadow copy
    # ------------------------------------------------------------------
    print(f"\nVSS shadow copy {shadow_id} still exists.")
    choice = input("Delete the shadow copy? (y/n): ").strip()
    if choice.lower() == "y":
        delete_vss_shadow(shadow_id)
        print("Shadow copy deleted.")
    else:
        print(f"Shadow copy preserved. ID: {shadow_id}")


def s3tovss(snapshot_prefix, s3_bucket, volume_letter=None,
            destination_region=None, endpoint_url=None, profile="default",
            resume=False, log_file=None):
    """Restore a VSS snapshot from S3 to a raw Windows volume.

    Workflow:
    1. Check platform (Windows) and privileges (Administrator).
    2. Enumerate and prompt for target volume selection if ``volume_letter`` is
       None.
    3. Confirm destructive overwrite with the user.
    4. Check target volume size against snapshot size.
    5. List S3 segments under ``snapshot_prefix``.
    6. Download, decompress, verify (SHA-256), and write segments in parallel.
    7. Write raw blocks to the target volume at correct offsets.

    Args:
        snapshot_prefix (str): S3 prefix identifying the snapshot
            (e.g. ``"vss-20260402-D.500"``).
        s3_bucket (str): Source S3 bucket name.
        volume_letter (str | None): Target drive letter (e.g. ``"E"``).  If
            None, the user is prompted interactively.
        destination_region (str | None): AWS region for the S3 bucket.
        endpoint_url (str | None): Custom S3 endpoint URL.
        profile (str): AWS CLI profile name.
        resume (bool): Resume a previously interrupted restore.
        log_file (str | None): Path for detailed per-segment log output.
    """
    _require_windows()
    _require_admin()

    from joblib import Parallel, delayed
    from singleton import SingletonClass
    singleton = SingletonClass()

    s3_region = (
        destination_region
        or singleton.AWS_DEST_REGION
        or singleton.AWS_ORIGIN_REGION
        or "us-east-1"
    )
    num_jobs = singleton.NUM_JOBS or 16

    # ------------------------------------------------------------------
    # Parse snapshot volume size from prefix ({snapshot_id}.{vol_gb})
    # ------------------------------------------------------------------
    try:
        snapshot_vol_gb = int(snapshot_prefix.rsplit(".", 1)[1])
    except (ValueError, IndexError):
        snapshot_vol_gb = None

    # ------------------------------------------------------------------
    # Volume selection
    # ------------------------------------------------------------------
    system_drive = os.environ.get("SystemDrive", "C:").rstrip(":").upper()

    if volume_letter is None:
        volumes = list_non_system_volumes()
        if not volumes:
            print("Error: No eligible volumes found.", file=sys.stderr)
            sys.exit(1)
        print(f"\nAvailable volumes (excluding system volume {system_drive}:):")
        for i, v in enumerate(volumes, 1):
            print(
                f"  [{i}] {v['letter']}:  {v['size_gb']} GB  "
                f"{v['filesystem']}  \"{v['label']}\""
            )
        while True:
            try:
                raw = input(f"\nSelect volume [1-{len(volumes)}]: ").strip()
                choice = int(raw) - 1
                if 0 <= choice < len(volumes):
                    volume_letter = volumes[choice]["letter"]
                    break
                print(f"Please enter a number between 1 and {len(volumes)}.")
            except ValueError:
                print("Invalid input.")
    else:
        volume_letter = volume_letter.upper().rstrip(":")
        if volume_letter == system_drive:
            print(
                f"Error: Cannot operate on system volume {volume_letter}:.",
                file=sys.stderr
            )
            sys.exit(1)

    # ------------------------------------------------------------------
    # Volume size check
    # ------------------------------------------------------------------
    try:
        target_vol_bytes = _get_volume_size_bytes(volume_letter)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    target_vol_gb = math.ceil(target_vol_bytes / (1024 ** 3))

    if snapshot_vol_gb is not None and target_vol_gb < snapshot_vol_gb:
        print(
            f"Error: Target volume {volume_letter}: ({target_vol_gb} GiB) is smaller "
            f"than snapshot size ({snapshot_vol_gb} GiB). Aborting.",
            file=sys.stderr
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Destructive overwrite warning + confirmation
    # ------------------------------------------------------------------
    print(
        f"\nWARNING: This will OVERWRITE all data on volume {volume_letter}: "
        f"({target_vol_gb} GiB) with snapshot '{snapshot_prefix}'."
    )
    print("This operation CANNOT be undone.")
    confirm = input(
        f"Type YES to confirm destructive overwrite of {volume_letter}:: "
    ).strip()
    if confirm != "YES":
        print("Aborted.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # List S3 segments
    # ------------------------------------------------------------------
    import boto3
    session = boto3.Session(profile_name=profile)
    s3_client = session.client("s3", region_name=s3_region, endpoint_url=endpoint_url)

    prefix = f"{snapshot_prefix}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    s3_keys = []
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".zstd"):
                s3_keys.append(key)

    if not s3_keys:
        print(
            f"Error: No segments found under s3://{s3_bucket}/{prefix}",
            file=sys.stderr
        )
        sys.exit(1)

    total_segments = len(s3_keys)
    print(f"Found {total_segments} segments to restore.")

    # ------------------------------------------------------------------
    # Resume manifest
    # ------------------------------------------------------------------
    manifest_path = _resume_manifest_path(snapshot_prefix)
    completed_offsets = set()

    if resume:
        existing = _find_restore_resume_manifest(snapshot_prefix, s3_bucket)
        if existing:
            choice = input(
                f"Resume previous restore of '{snapshot_prefix}' "
                f"to {volume_letter}:? (y/n): "
            ).strip()
            if choice.lower() == "y":
                completed_offsets = set(existing.get("completed_segments", []))
                print(
                    f"Resuming restore, {len(completed_offsets)} segments already written."
                )

    _write_restore_resume_manifest(
        manifest_path, snapshot_prefix, volume_letter,
        s3_bucket, total_segments, completed_offsets
    )

    # Filter out already-completed segments by offset
    pending_keys = []
    for key in s3_keys:
        key_name = key.split("/", 1)[1] if "/" in key else key
        try:
            offset = int(key_name.split(".")[0])
        except (ValueError, IndexError):
            offset = -1
        if offset not in completed_offsets:
            pending_keys.append(key)

    # ------------------------------------------------------------------
    # Parallel restore — process in batches for progress display
    # ------------------------------------------------------------------
    display_vol_gb = snapshot_vol_gb or target_vol_gb
    print(f"Restoring to {volume_letter}:...")
    start_time = time.perf_counter()
    batch_size = max(num_jobs * 4, 64)

    try:
        with Parallel(n_jobs=num_jobs) as parallel:
            for batch_start in range(0, len(pending_keys), batch_size):
                batch = pending_keys[batch_start: batch_start + batch_size]
                results = parallel(
                    delayed(_restore_s3_segment)(
                        key, s3_bucket, volume_letter, s3_region,
                        endpoint_url, profile, log_file
                    )
                    for key in batch
                )
                for offset in results:
                    completed_offsets.add(offset)

                _write_restore_resume_manifest(
                    manifest_path, snapshot_prefix, volume_letter,
                    s3_bucket, total_segments, completed_offsets
                )

                done = len(completed_offsets)
                elapsed = time.perf_counter() - start_time
                bytes_done = done * 64 * CHUNK_SIZE
                speed = bytes_done / elapsed if elapsed > 0 else 0
                remaining = total_segments - done
                eta_s = (
                    remaining * 64 * CHUNK_SIZE / speed
                    if speed > 0 else 0
                )
                print(
                    f"\rProgress: {100.0 * done / total_segments:.0f}% "
                    f"({bytes_done // (1024 ** 3)} GB / {display_vol_gb} GB) | "
                    f"Speed: {speed / (1024 ** 2):.0f} MB/s | "
                    f"ETA: {int(eta_s // 60)}m {int(eta_s % 60)}s",
                    end="", flush=True
                )

    except Exception as exc:
        print(f"\nError during restore: {exc}", file=sys.stderr)
        print(
            f"Restore interrupted. Resume manifest saved to: {manifest_path}",
            file=sys.stderr
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Success
    # ------------------------------------------------------------------
    elapsed = time.perf_counter() - start_time
    total_bytes = total_segments * 64 * CHUNK_SIZE
    speed = total_bytes / elapsed if elapsed > 0 else 0
    print(
        f"\nRestore complete. "
        f"Took {elapsed:.1f}s at {speed / (1024 ** 2):.1f} MB/s."
    )

    try:
        os.remove(manifest_path)
    except OSError:
        pass
