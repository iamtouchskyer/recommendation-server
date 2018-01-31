import unittest
import os
import sys

import uploader.info_uploader

# import from top-level
sys.path.insert(0, '..')

class MockInfoUploader(uploader.info_uploader.InfoUploader):
  def consumer(self, data):
    if isinstance(data, list) and 'fid' in data[0]:
      self.setStatus('ok')
    else:
      self.setStatus('failed')

class TestInfoUploader(unittest.TestCase):
  def setUp(self):
    self._uploader = MockInfoUploader()
    self.testInfoDir = os.path.abspath('uploader/tests/data/videoinfo')

  def test_upload(self):
    self._uploader.start(info_dir=self.testInfoDir, info_type='image')

    self.assertEqual('ok', self._uploader.getStatus())

if __name__ == '__main__':
  unittest.main()