import argparse
import json

import pymongo

import folder_processor
import uploader

class InfoUploader(uploader.BulkUploader):
    def __init__(self):
        uploader.BulkUploader.__init__(self, queue_size=1000)
        self._info_coll = self.get_mongo_collection('videoimage')
        self._counter = 0

    def get_mongo_collection(self, collection_name):
        uri = 'mongodb://stcarecdb:xAQCuSzu4jezWd5zSOSmcBlMfX3hbAuxoFu1yeJDgEOCCDrJLYiMs9CVbX5NfQ7vZ2nfL2SQj4yIfy3WTkYOVA==@stcarecdb.documents.azure.com:10255/?ssl=true&replicaSet=globaldb'
        client = pymongo.MongoClient(uri)
        db = client.playback_events
        return db[collection_name]

    def producer(self, info_dir, info_type):
        """
        Uploads certain videoinfo type in folder info_dir

        Keyword arguments:
        info_dir -- The folder holding videoinfo
        info_type -- video info type, e.g., "parent", "image", "child", etc...
        """
        folder_processor.walk_dir(
            info_dir, 
            '^.*/' + info_type + '/.*$', 
            self.info_processor)

    def info_processor(self, full_path):
        # reads the content of the file in JSON
        if full_path:
            with open(full_path) as json_data:
                d = json.load(json_data)
                del d['_id']
                self.submit(d)
        else:
            self.submit(None)

    def consumer(self, data):
        result = self._info_coll.insert_many(data)
        self._counter += len(result.inserted_ids)

        print self._counter

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='VideoInfo uploader')
    PARSER.add_argument('--dir', dest='videoinfo_dir')
    PARSER.add_argument('--type', dest='videoinfo_type')

    ARGS = PARSER.parse_args()

    uploader = InfoUploader()
    uploader.start(info_dir=ARGS.videoinfo_dir, info_type=ARGS.videoinfo_type)
