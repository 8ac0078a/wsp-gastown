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
import json
import os
import platform
import subprocess
import sys
from ctypes import wintypes  # noqa: F401 — used by Win32 API callers


def _require_windows():
    """Exit with a clean message if not running on Windows."""
    if platform.system() != "Windows":
        print("Error: This command is only supported on Windows.", file=sys.stderr)
        sys.exit(1)


def _require_admin():
    """Exit with a clean message if not running as Administrator."""
    raise NotImplementedError


def list_non_system_volumes():
    """Return a list of non-system, non-recovery volumes available for backup/restore.

    Each entry is a dict with keys: letter, size_gb, filesystem, label.
    Excludes the system volume (os.environ['SystemDrive']) and volumes without
    a drive letter.

    Returns:
        list[dict]: Available volumes.
    """
    raise NotImplementedError


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
    raise NotImplementedError


def delete_vss_shadow(shadow_id):
    """Delete a VSS shadow copy by its ID.

    Runs ``vssadmin delete shadows /shadow=<shadow_id> /quiet``.

    Args:
        shadow_id (str): GUID string returned by :func:`create_vss_shadow`.
    """
    raise NotImplementedError


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
    raise NotImplementedError


def read_raw_blocks(handle, offset_bytes, length_bytes):
    """Read raw bytes from an open device handle.

    Args:
        handle (int): Win32 HANDLE from :func:`open_shadow_device`.
        offset_bytes (int): Byte offset to seek to before reading.
        length_bytes (int): Number of bytes to read (must be sector-aligned).

    Returns:
        bytes: Raw block data.
    """
    raise NotImplementedError


def close_device_handle(handle):
    """Close a Win32 device handle.

    Args:
        handle (int): Win32 HANDLE from :func:`open_shadow_device`.
    """
    raise NotImplementedError


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
    raise NotImplementedError


def s3tovss(snapshot_prefix, s3_bucket, volume_letter=None,
            destination_region=None, endpoint_url=None, profile="default",
            resume=False, log_file=None):
    """Restore a VSS snapshot from S3 to a raw Windows volume.

    Workflow:
    1. Check platform (Windows) and privileges (Administrator).
    2. Enumerate and prompt for target volume selection if ``volume_letter`` is
       None.
    3. Confirm destructive overwrite with the user.
    4. List S3 segments under ``snapshot_prefix``.
    5. Download, decompress, and verify segments in parallel.
    6. Write raw blocks to the target volume at correct offsets.

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
    raise NotImplementedError
