# This module processes a given folder.
# During the processing, whenever we encounter a file,
# we'll invoke a callback to process that file, regardless
# of the preception how it will be processed

import os
import re

def walk_dir(base_dir, pattern, processor):
  """ 
  Walks in a folder and its subfolders recursively.
  For each file found in base_dir, we'll call processor with
  full path to the file to handle it.

  When calling processor with None, it means the walk has ended

  Keyword agruments:
  base_dir -- the absolute path to the folder.
  pattern -- a regex to match fullpath.
  processor -- a function that will be called to process the file.
  """

  # compile regex firsthand
  prog = re.compile(pattern)

  for dirpath, dirnames, filenames in os.walk(base_dir):
    if filenames:
      for filename in filenames:
        full_path = os.path.join(dirpath, filename)

        if prog.match(full_path):
          processor(full_path)

  processor(None)