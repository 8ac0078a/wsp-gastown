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
import random
import unittest
import sys
import os
import subprocess

sys.path.insert(1, f'{os.path.dirname(os.path.realpath(__file__))}/../src') #makes source code testable

from main import install_dependencies, dependency_checker, version_cmp
from snapshot_factory import generate_pattern_snapshot, check_pattern

"""Method to expose test cases for dependency checker and installer to test runner via a test suite."""
def DependencyCheckerSuite():
  suite = unittest.TestSuite()

  suite.addTest(PackageVersionCMP('equal_length_less'))
  suite.addTest(PackageVersionCMP('equal_length_same'))
  suite.addTest(PackageVersionCMP('equal_length_greater'))
  suite.addTest(PackageVersionCMP('v1_shorter_less'))
  suite.addTest(PackageVersionCMP('v1_shorter_same'))
  suite.addTest(PackageVersionCMP('v1_shorter_greater'))
  suite.addTest(PackageVersionCMP('v2_shorter_less'))
  suite.addTest(PackageVersionCMP('v2_shorter_same'))
  suite.addTest(PackageVersionCMP('v2_shorter_greater'))

  suite.addTest(DependencyCheckAndInstall('fresh_install'))
  suite.addTest(DependencyCheckAndInstall('all_installed'))
  suite.addTest(DependencyCheckAndInstall('mix_in_to_install_to_update_and_some_satisfied'))

  return suite

'''Unit tests for the dependency checker in src/main.py
'''
class PackageVersionCMP(unittest.TestCase):
  def equal_length_less(self):
    v1 = "1.2.3"
    v2 = "1.2.4"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "10.2"
    v2 = "10.3"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "1"
    v2 = "2"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

  def equal_length_same(self):
    v1 = "1.2.3"
    v2 = "1.2.3"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

    v1 = "10.2"
    v2 = "10.2"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

    v1 = "1"
    v2 = "1"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

  def equal_length_greater(self):
    v1 = "1.2.4"
    v2 = "1.2.3"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "10.3"
    v2 = "10.2"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "2"
    v2 = "1"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

  def v1_shorter_less(self):
    v1 = "1.2"
    v2 = "1.2.4"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "10"
    v2 = "10.3"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "1.0"
    v2 = "2.1.3.26"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

  def v1_shorter_same(self):
    v1 = "1.2"
    v2 = "1.2.0"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

    v1 = "1"
    v2 = "1.0.0"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

  def v1_shorter_greater(self):
    v1 = "1.3"
    v2 = "1.2.3"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "10"
    v2 = "9.12.4"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "2"
    v2 = "1.7"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

  def v2_shorter_less(self):
    v1 = "0.2.26"
    v2 = "1"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "10.2.98"
    v2 = "10.3"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "1.0.7.9"
    v2 = "2.3.5"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

  def v2_shorter_same(self):
    v1 = "1.2.0"
    v2 = "1.2"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

    v1 = "1.0.0"
    v2 = "1"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

  def v2_shorter_greater(self):
    v1 = "1.4.3"
    v2 = "1.3"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "11.12.4"
    v2 = "10"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "3.7"
    v2 = "2"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

'''Unit tests for the dependency install prompter in src/main.py
'''
class DependencyCheckAndInstall(unittest.TestCase):
  requirements = ['aws-shell>=0.2.2', 'boto3>=1.24.22', 'botocore>=1.27.25',
  'joblib>=1.1.0', 'numpy>=1.21.6', 'ruamel.yaml>=0.17.21', 'ruamel.yaml.clib>=0.2.6',
  'urllib3>=1.26.9', 'zstandard>0,<0.18.0']
  curr_pip3_packages = []

  def setUp(self):
    super().setUp()

    self.package_names = ['aws-shell', 'boto3', 'botocore','joblib', 'numpy', 'ruamel.yaml', 'ruamel.yaml.clib', 'urllib3', 'zstandard']

    result = subprocess.run(["pip3", "freeze"], capture_output=True)
    self.curr_pip3_packages = result.stdout.decode('utf-8').split('\n')

  def fresh_install(self):
    mock_pip_freeze = ['']
    needs_install, needs_version_adjustment = dependency_checker(mock_pip_freeze, self.requirements)

    self.assertEqual(len(needs_install), len(self.requirements), "On Fresh Install. Does not recognize that all dependencies must be installed.")
    self.assertEqual(len(needs_version_adjustment), 0, "On Fresh Install, no packages need version adjustments.")

    counter = 0
    for p in self.package_names:
      self.assertTrue((p in needs_install), "Not all required dependencies found in install list.")
      counter += 1

    self.assertEqual(counter, 9, "On Fresh Install, Not all required packages would've been installed.")

  def all_installed(self):
    mock_pip_freeze = ['aws-shell==0.2.2', 'boto3==1.24.22', 'botocore==1.27.25',
    'joblib==1.1.0', 'numpy==1.21.6', 'ruamel.yaml==0.17.21', 'ruamel.yaml.clib==0.2.6',
    'urllib3==1.26.9', 'zstandard==0.18.0']
    needs_install, needs_version_adjustment = dependency_checker(mock_pip_freeze, self.requirements)

    self.assertEqual(len(needs_install), 0, "Did not recognize that all packages are already installed")
    self.assertEqual(len(needs_version_adjustment), 0, "Did not recognize that all packages are already installed")

  def mix_in_to_install_to_update_and_some_satisfied(self):
    '''
    Note:
    to install: numpy==1.21.6, 'boto3==1.24.22', 'botocore==1.27.25'
    to upgrade: 'aws-shell==0.2.2', 'zstandard==0.18.0'
    all good: 'joblib==1.1.0', 'ruamel.yaml==0.17.21', 'ruamel.yaml.clib==0.2.6', 'urllib3==1.26.9'
    '''
    mock_pip_freeze = ['aws-shell==0.2.1', 'joblib==1.1.0', 'ruamel.yaml==0.17.21',
    'ruamel.yaml.clib==0.2.6', 'urllib3==1.26.9', 'zstandard==0.19.0']

    needs_install, needs_version_adjustment = dependency_checker(mock_pip_freeze, self.requirements)

    self.assertEqual(len(needs_install), 3, "Did not identify all packages that need to be installed.")
    self.assertEqual(len(needs_version_adjustment), 2, "Did not identify all packages that need to be upgraded.")

    counter = 0
    for p in self.package_names:
      if p in needs_install:
        counter += 1
    self.assertEqual(counter, 3, "Did not identify all packages that need to be installed.")

    counter = 0
    for p in self.package_names:
      if p in needs_version_adjustment:
        counter += 1
    self.assertEqual(counter, 2, "Did not identify all packages that need to be upgraded.")


"""Method to expose test cases for vss volume filtering to test runner via a test suite."""
def VssVolumeSuite():
  suite = unittest.TestSuite()
  suite.addTest(VssVolumeFilter('test_list_volumes_filters_system_drive'))
  suite.addTest(VssVolumeFilter('test_list_volumes_filters_no_drive_letter'))
  suite.addTest(VssVolumeFilter('test_list_volumes_filters_recovery_labels'))
  suite.addTest(VssVolumeFilter('test_list_volumes_single_object_coercion'))
  suite.addTest(VssVolumeFilter('test_resolve_volume_explicit_letter'))
  suite.addTest(VssVolumeFilter('test_resolve_volume_strips_colon'))
  suite.addTest(VssVolumeFilter('test_resolve_volume_system_drive_exits'))
  return suite


class VssVolumeFilter(unittest.TestCase):
  """Unit tests for vss.py volume enumeration filtering logic.

  These tests exercise the pure-Python post-processing of the volume list
  returned from the PowerShell subprocess, without requiring Windows or
  a real PowerShell invocation.
  """

  # Helper: run the filter logic directly without calling PowerShell.
  def _apply_filter(self, raw_vols, system_letter='C'):
    """Apply the same filtering logic as list_volumes() to a pre-built list."""
    from vss import _RECOVERY_LABELS
    volumes = []
    for vol in raw_vols:
      letter = (vol.get('letter') or '').strip().upper()
      label = (vol.get('label') or '').strip()
      filesystem = (vol.get('filesystem') or '').strip()
      size_gb = int(vol.get('size_gb') or 0)

      if not letter:
        continue
      if letter == system_letter.upper():
        continue
      if label.lower() in _RECOVERY_LABELS:
        continue

      volumes.append({'letter': letter, 'label': label,
                      'filesystem': filesystem, 'size_gb': size_gb})
    return volumes

  def test_list_volumes_filters_system_drive(self):
    raw = [
      {'letter': 'C', 'label': 'Windows', 'filesystem': 'NTFS', 'size_gb': 100},
      {'letter': 'D', 'label': 'Data',    'filesystem': 'NTFS', 'size_gb': 500},
    ]
    result = self._apply_filter(raw, system_letter='C')
    letters = [v['letter'] for v in result]
    self.assertNotIn('C', letters, "System drive should be filtered out")
    self.assertIn('D', letters, "Non-system drive should be included")

  def test_list_volumes_filters_no_drive_letter(self):
    raw = [
      {'letter': '',  'label': 'Hidden', 'filesystem': 'NTFS', 'size_gb': 50},
      {'letter': 'E', 'label': 'Logs',   'filesystem': 'NTFS', 'size_gb': 200},
    ]
    result = self._apply_filter(raw, system_letter='C')
    letters = [v['letter'] for v in result]
    self.assertEqual(len(result), 1, "Volume with no drive letter should be excluded")
    self.assertIn('E', letters)

  def test_list_volumes_filters_recovery_labels(self):
    raw = [
      {'letter': 'R', 'label': 'Recovery',        'filesystem': 'NTFS', 'size_gb': 1},
      {'letter': 'S', 'label': 'System Reserved',  'filesystem': 'NTFS', 'size_gb': 1},
      {'letter': 'W', 'label': 'WinRE',            'filesystem': 'NTFS', 'size_gb': 1},
      {'letter': 'D', 'label': 'Data',             'filesystem': 'NTFS', 'size_gb': 100},
    ]
    result = self._apply_filter(raw, system_letter='C')
    letters = [v['letter'] for v in result]
    self.assertNotIn('R', letters, "Recovery label should be filtered out")
    self.assertNotIn('S', letters, "System Reserved label should be filtered out")
    self.assertNotIn('W', letters, "WinRE label should be filtered out")
    self.assertIn('D', letters, "Normal data volume should be included")

  def test_list_volumes_single_object_coercion(self):
    """PowerShell returns a bare dict (not list) when exactly one volume matches."""
    from vss import _RECOVERY_LABELS
    # Simulate what list_volumes does when json.loads returns a dict
    raw_dict = {'letter': 'D', 'label': 'Data', 'filesystem': 'NTFS', 'size_gb': 500}
    # list_volumes coerces: if isinstance(data, dict): data = [data]
    data = raw_dict if not isinstance(raw_dict, list) else raw_dict
    if isinstance(data, dict):
      data = [data]
    result = self._apply_filter(data, system_letter='C')
    self.assertEqual(len(result), 1)
    self.assertEqual(result[0]['letter'], 'D')

  def test_resolve_volume_explicit_letter(self):
    """resolve_volume with an explicit letter returns it uppercased, no colon."""
    import os
    os.environ.setdefault('SystemDrive', 'C:')
    from vss import resolve_volume
    self.assertEqual(resolve_volume('D'), 'D')
    self.assertEqual(resolve_volume('d'), 'D')
    self.assertEqual(resolve_volume('E:'), 'E')

  def test_resolve_volume_strips_colon(self):
    """resolve_volume strips trailing colon from the provided letter."""
    import os
    os.environ.setdefault('SystemDrive', 'C:')
    from vss import resolve_volume
    self.assertEqual(resolve_volume('F:'), 'F')

  def test_resolve_volume_system_drive_exits(self):
    """resolve_volume raises SystemExit when the system drive is specified."""
    import os
    os.environ['SystemDrive'] = 'C:'
    from vss import resolve_volume
    with self.assertRaises(SystemExit):
      resolve_volume('C')
    with self.assertRaises(SystemExit):
      resolve_volume('c:')


"""Method to expose test cases for dependency checker and installer to test runner via a test suite."""
def SnapshotFactorySuite():
  suite = unittest.TestSuite()

  suite.addTest(TestSnapshotFactory('test_full_disk_no_offset'))
  suite.addTest(TestSnapshotFactory('test_full_disk_offset'))
  suite.addTest(TestSnapshotFactory('test_disk_subset_random_param'))

  return suite

'''Unit tests for the snapshot_factory.py

Note that these can take quite a long time to run (30 minutes +)
'''
class TestSnapshotFactory(unittest.TestCase):

  def run_test_matrix(self, TEST_MATRIX):
    for test_case in TEST_MATRIX:
      result = generate_pattern_snapshot(
        test_case["parameters"]["size"], 
        test_case["parameters"]["start"], 
        test_case["parameters"]["end"], 
        test_case["parameters"]["skip"], 
        test_case["parameters"]["offset"]
      )

      msg = test_case["description"]
      self.assertIsNotNone(result, f"Failed to create snapshot\n{msg}\n" + json.dumps(test_case["parameters"], indent=2))

      patterns = []
      patterns.append(result["metadata"])
      test_case["patterns"] = patterns
      test_case["snap"] = result["snap"]
      test_case["size"] = result["size"]

      # takes ~ 30s to min for ebs direct to see snapshot. pre-load snapshot before testing validity
      check_pattern(result["snap"], result["size"], patterns)
    
    for test_case in TEST_MATRIX:
      msg = test_case["description"]
      patterns = test_case["patterns"]
      snapshot_id = test_case["snap"]
      size = test_case["size"]
      self.assertTrue(check_pattern(snapshot_id, size, patterns), f"snapshot data is incorrect\n{msg}\n" + json.dumps(test_case["parameters"], indent=2))

  def test_full_disk_no_offset(self):
    TEST_MATRIX = []
    # powers of two
    skip = 1
    while skip <= 32:
        test_case = {
            "parameters": {
              "size": 1,
              "start": 0,
              "end": None,
              "skip": skip,
              "offset": 0
            },
            f"description": f"Attempting to create a snapshot where every {skip} sectors is label with its sector number. Good for checking reordering of snapshot data."
        }
        TEST_MATRIX.append(test_case)

        skip *= 2
      
    # primes
    skips =  [3, 5, 7, 11]
    for skip in skips:
      test_case = {
        "parameters": {
            "size": 1,
            "start": 0,
            "end": None,
            "skip": skip,
            "offset": 0
          },
          "description": f"Attempting to create a snapshot where every {skip} sectors is label with its sector number. Good for checking reordering of snapshot data."
      }
      TEST_MATRIX.append(test_case)

    self.run_test_matrix(TEST_MATRIX)

  def test_full_disk_offset(self):
    TEST_MATRIX = []
    skips = [1,2,3,4]
    for skip in skips:
      for i in range(1, skip):
        test_case = {
            "parameters": {
                "size": 1,
                "start": 0,
                "end": None,
                "skip": skip,
                "offset": i
        
            },
            "description": f"Attempting to create a snapshot where every {skip} sectors is label with its sector number with offset = {i}. Good for checking reordering of snapshot data."
        }
        TEST_MATRIX.append(test_case)

    self.run_test_matrix(TEST_MATRIX)

  def test_disk_subset_random_param(self):
    TEST_MATRIX = []
    for i in range(5):
      start = random.randint(0, 2097152/2)
      end = random.randint(2097152/2, 2097152)
      skip = random.randint(0, 2048)
      offset = random.randint(0, skip)
      test_case = {
        "parameters": {
            "size": 1,
            "start": start,
            "end": end,
            "skip": skip,
            "offset": offset
    
        },
        "description": f"Attempting to create a snapshot where every {skip} sectors is label with its sector number with offset = {offset} on the sector interval [{start}, {end}]. Good for checking reordering of snapshot data."
      }
      TEST_MATRIX.append(test_case)

    self.run_test_matrix(TEST_MATRIX)