import unittest
import os
import sys

import uploader.folder_processor

# import from top-level
sys.path.insert(0, '..')

class TestEventFolderProcessor(unittest.TestCase):
  def setUp(self):
    self.testEventDir = os.path.abspath('uploader/tests/data/events')
    self.testInfoDir = os.path.abspath('uploader/tests/data/videoinfo')
    self.walkResult = []

  def test_walk_event_dir(self):
    uploader.folder_processor.walk_dir(self.testEventDir, '(.*)\.txt$', self.mock_event_processor)

  def test_walk_info_dir(self):
    uploader.folder_processor.walk_dir(self.testInfoDir, '^.*/image/.*$', self.mock_info_processor)

  def mock_event_processor(self, fullpath):
    if fullpath:
      # when fullpath is not None, we should append it to self.walkResult
      # as we will be called multiple times and we should check the result
      # when the walk completes.
      self.walkResult.append(fullpath)
    else:
      self.assertEqual(self.walkResult, [
        os.path.join(self.testEventDir, '2.txt'),
        os.path.join(self.testEventDir, '1.txt')
      ])

  def mock_info_processor(self, fullpath):
    if fullpath:
      self.walkResult.append(fullpath)
    else:
      self.assertEqual(self.walkResult, [
        os.path.join(self.testInfoDir, '31', '203569', 'image', '468efa299de7589724b0363e63d9f259.json')
      ])

if __name__ == '__main__':
  unittest.main()