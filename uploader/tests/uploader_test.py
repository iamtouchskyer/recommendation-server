import unittest
import os
import sys

import uploader.uploader

# import from top-level
sys.path.insert(0, '..')

class MockUploader(uploader.uploader.Uploader):
  def producer(self, root, data):
    self.submit(data)

  def consumer(self, data):
    if 'data' in data:
      self.setStatus('ok')
    else:
      self.setStatus('error')

class TestUploader(unittest.TestCase):
  def setUp(self):
    self._uploader = MockUploader()

  def test_upload(self):
    self._uploader.start(root='root', data='data')

    self.assertEqual('ok', self._uploader.getStatus())

if __name__ == '__main__':
  unittest.main()