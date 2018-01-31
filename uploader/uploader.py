import Queue

class Uploader:
  def __init__(self):
    self._status = None

  def consumer(self, data):
    pass

  def producer(self, **args):
    pass

  def submit(self, data):
    self.consumer(data)

  def getStatus(self):
    return self._status

  def setStatus(self, status):
    self._status = status

  def start(self, **args):
   self.producer(**args)

class BulkUploader(Uploader):
  def __init__(self, queue_size=1):
    Uploader.__init__(self)

    self._queue = []
    self._queue_size = queue_size
  
  def submit(self, data):
    if data is None:
      if self._queue:
        self.consumer(self._queue)

      return

    self._queue.append(data)

    if len(self._queue) == self._queue_size:
      self.consumer(self._queue)
      self._queue = []