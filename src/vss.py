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

import json
import os
import subprocess
import sys


# Labels that identify recovery/reserved partitions (case-insensitive)
_RECOVERY_LABELS = frozenset(['recovery', 'system reserved', 'winre'])


def list_volumes():
    """List non-system, non-recovery Windows volumes with drive letters.

    Uses WMI Win32_Volume via PowerShell to enumerate fixed local disks,
    then filters out:
      - Volumes with no drive letter
      - The system volume (os.environ['SystemDrive'])
      - Recovery and system-reserved partitions (by label)

    Returns:
        list of dicts with keys:
            letter     (str)  — single uppercase letter, e.g. 'D'
            label      (str)  — volume label, may be empty
            filesystem (str)  — e.g. 'NTFS', 'ReFS'
            size_gb    (int)  — size in GiB, rounded

    Raises:
        RuntimeError: if not running on Windows, PowerShell is unavailable,
                      enumeration times out, or output cannot be parsed.
    """
    if sys.platform != 'win32':
        raise RuntimeError("Volume enumeration is only supported on Windows.")

    # Build a self-contained PowerShell script that outputs JSON.
    # DriveType 3 == Fixed (local disk). We exclude removable, CD-ROM, etc.
    ps_script = (
        "$ErrorActionPreference = 'Stop'; "
        "$vols = Get-WmiObject -Class Win32_Volume "
        "| Where-Object { $_.DriveType -eq 3 -and $_.DriveLetter -ne $null -and $_.DriveLetter.Trim() -ne '' } "
        "| ForEach-Object { "
        "    $letter = $_.DriveLetter.TrimEnd('\\').TrimEnd(':'); "
        "    [PSCustomObject]@{ "
        "        letter = $letter; "
        "        label = if ($_.Label) { $_.Label } else { '' }; "
        "        filesystem = if ($_.FileSystem) { $_.FileSystem } else { '' }; "
        "        size_gb = [long][math]::Round($_.Capacity / 1073741824, 0) "
        "    } "
        "}; "
        "if ($null -eq $vols) { $vols = @() }; "
        "$vols | ConvertTo-Json -Compress"
    )

    try:
        result = subprocess.run(
            ['powershell', '-NoProfile', '-NonInteractive', '-Command', ps_script],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        raise RuntimeError("PowerShell not found. Cannot enumerate volumes.")
    except subprocess.TimeoutExpired:
        raise RuntimeError("Volume enumeration timed out after 30 seconds.")

    if result.returncode != 0:
        raise RuntimeError(
            f"Volume enumeration failed (exit {result.returncode}): "
            f"{result.stderr.strip()}"
        )

    stdout = result.stdout.strip()
    if not stdout:
        return []

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse volume list output: {exc}")

    # PowerShell returns a bare object (not array) when exactly one item matches.
    if isinstance(data, dict):
        data = [data]

    system_letter = os.environ.get('SystemDrive', 'C:').rstrip('\\').rstrip(':').upper()

    volumes = []
    for vol in data:
        letter = (vol.get('letter') or '').strip().upper()
        label = (vol.get('label') or '').strip()
        filesystem = (vol.get('filesystem') or '').strip()
        size_gb = int(vol.get('size_gb') or 0)

        if not letter:
            continue  # no drive letter — skip

        if letter == system_letter:
            continue  # system volume — skip

        if label.lower() in _RECOVERY_LABELS:
            continue  # recovery / system-reserved partition — skip

        volumes.append({
            'letter': letter,
            'label': label,
            'filesystem': filesystem,
            'size_gb': size_gb,
        })

    return volumes


def prompt_volume_selection(volumes):
    """Present a numbered list of volumes and prompt the user to select one.

    Args:
        volumes: list of dicts as returned by list_volumes()

    Returns:
        Single uppercase drive letter string, e.g. 'D'.

    Raises:
        SystemExit(1): if volumes list is empty or user aborts (Ctrl-C / EOF).
    """
    system_drive = os.environ.get('SystemDrive', 'C:').rstrip('\\')

    if not volumes:
        print(
            f"No eligible volumes found (system volume {system_drive} excluded).",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\nAvailable volumes (excluding system volume {system_drive}):")
    for i, vol in enumerate(volumes, 1):
        label_str = f'  "{vol["label"]}"' if vol['label'] else ''
        print(f"  [{i}] {vol['letter']}:  {vol['size_gb']} GB  {vol['filesystem']}{label_str}")

    while True:
        try:
            choice = input(f"\nSelect volume [1-{len(volumes)}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.", file=sys.stderr)
            sys.exit(1)

        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(volumes):
                return volumes[idx - 1]['letter']

        print(f"Invalid selection. Enter a number between 1 and {len(volumes)}.")


def resolve_volume(volume_arg):
    """Resolve the target volume letter from CLI arg or interactive selection.

    If volume_arg is provided, validates it is not the system drive.
    If volume_arg is None, enumerates eligible volumes and prompts the user.

    Args:
        volume_arg: drive letter string from --volume flag (e.g. 'D' or 'D:'),
                    or None to trigger interactive selection.

    Returns:
        Single uppercase drive letter string (no colon), e.g. 'D'.

    Raises:
        SystemExit(1): on system-volume conflict, enumeration failure, or user abort.
    """
    system_drive = os.environ.get('SystemDrive', 'C:').rstrip('\\')
    system_letter = system_drive.rstrip(':').upper()

    if volume_arg is not None:
        letter = volume_arg.strip().rstrip(':').upper()
        if letter == system_letter:
            print(f"Cannot operate on system volume {system_drive}.", file=sys.stderr)
            sys.exit(1)
        return letter

    try:
        volumes = list_volumes()
    except RuntimeError as exc:
        print(f"Error enumerating volumes: {exc}", file=sys.stderr)
        sys.exit(1)

    return prompt_volume_selection(volumes)
