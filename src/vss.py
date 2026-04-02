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

import re
import subprocess
import sys


def create_shadow_copy(volume_letter):
    """Create a VSS shadow copy for the given volume.

    Runs 'vssadmin create shadow /for=<LETTER>:' and parses the output for
    the Shadow Copy ID (GUID) and Shadow Copy Volume Name (device path).

    Args:
        volume_letter: Single drive letter, e.g. 'D'.

    Returns:
        Tuple (shadow_id, device_path) on success.
        shadow_id example:  '{AB12CD34-1234-5678-ABCD-1234567890AB}'
        device_path example: '\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1'

    Raises:
        SystemExit: If vssadmin fails or output cannot be parsed.
    """
    volume = f"{volume_letter.upper()}:"
    print(f"Creating VSS shadow copy of {volume}...")

    try:
        result = subprocess.run(
            ["vssadmin", "create", "shadow", f"/for={volume}"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print("Error: vssadmin not found. This command requires Windows.", file=sys.stderr)
        sys.exit(1)

    output = result.stdout + result.stderr

    if result.returncode != 0:
        print(f"Error: vssadmin failed (exit {result.returncode}):", file=sys.stderr)
        print(output, file=sys.stderr)
        sys.exit(1)

    shadow_id, device_path = _parse_create_output(output)

    if shadow_id is None or device_path is None:
        print("Error: Failed to parse vssadmin output:", file=sys.stderr)
        print(output, file=sys.stderr)
        sys.exit(1)

    print(f"Shadow copy created: {shadow_id}")
    return shadow_id, device_path


def _parse_create_output(output):
    """Parse vssadmin create shadow output.

    Args:
        output: Combined stdout/stderr string from vssadmin.

    Returns:
        Tuple (shadow_id, device_path), either element is None if not found.
        shadow_id is the GUID string including braces, e.g.
            '{AB12CD34-1234-5678-ABCD-1234567890AB}'
        device_path is the UNC device path, e.g.
            '\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1'
    """
    shadow_id = None
    device_path = None

    # Match: Shadow Copy ID: {GUID}
    id_match = re.search(
        r"Shadow Copy ID:\s*(\{[0-9A-Fa-f\-]+\})",
        output,
        re.IGNORECASE,
    )
    if id_match:
        shadow_id = id_match.group(1)

    # Match: Shadow Copy Volume Name: \\?\GLOBALROOT\Device\HarddiskVolumeShadowCopyN
    path_match = re.search(
        r"Shadow Copy Volume Name:\s*(\\\\[^\s]+)",
        output,
        re.IGNORECASE,
    )
    if path_match:
        device_path = path_match.group(1).rstrip("\\")

    return shadow_id, device_path


def delete_shadow_copy(shadow_id):
    """Delete a VSS shadow copy by ID.

    Runs 'vssadmin delete shadows /shadow=<ID> /quiet'.

    Args:
        shadow_id: Shadow Copy GUID string, e.g.
            '{AB12CD34-1234-5678-ABCD-1234567890AB}'.

    Returns:
        True on success, False on failure.
    """
    try:
        result = subprocess.run(
            ["vssadmin", "delete", "shadows", f"/shadow={shadow_id}", "/quiet"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print("Error: vssadmin not found. This command requires Windows.", file=sys.stderr)
        return False

    if result.returncode != 0:
        print(f"Error: Failed to delete shadow copy {shadow_id}:", file=sys.stderr)
        print(result.stdout + result.stderr, file=sys.stderr)
        return False

    return True


def prompt_delete_shadow_copy(shadow_id):
    """Prompt the user to delete a VSS shadow copy after a successful upload.

    Per DESIGN.md §4.8: if user chooses 'y', delete the shadow copy;
    if 'n', print the ID for manual cleanup.

    Args:
        shadow_id: Shadow Copy GUID string.
    """
    print(f"\nUpload complete. VSS shadow copy {shadow_id} still exists.")
    choice = input("Delete the shadow copy? (y/n): ").strip().lower()
    if choice == "y":
        if delete_shadow_copy(shadow_id):
            print("Shadow copy deleted.")
        else:
            print(f"Failed to delete shadow copy. Delete manually: vssadmin delete shadows /shadow={shadow_id} /quiet")
    else:
        print(f"Shadow copy retained. To delete later: vssadmin delete shadows /shadow={shadow_id} /quiet")
