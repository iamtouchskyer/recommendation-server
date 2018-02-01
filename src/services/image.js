const MongoClient = require('mongodb').MongoClient;

const encodedToken = encodeURIComponent('xAQCuSzu4jezWd5zSOSmcBlMfX3hbAuxoFu1yeJDgEOCCDrJLYiMs9CVbX5NfQ7vZ2nfL2SQj4yIfy3WTkYOVA==');

const url = 'mongodb://stcarecdb:' + encodedToken + '@stcarecdb.documents.azure.com:10255/?ssl=true&ssl_cert_reqs=CERT_NONE'

const generateImageUrl = (info) => {
  if (info.fid && info.height && info.width) {
    return 'http://cdn.cibn.cc/view/' + info.fid + '-' + info.width + '-' + info.width;
  }

  return null;
}

const getImageForVideo = (vid) => {
  console.log('Fetching vid ' + vid);

  return new Promise((resolve, reject) => {
    MongoClient.connect(url, (err, client) => {
      if (err) {
        reject(err);
      }

      const db = client.db('playback_events');
      const video_collection = db.collection('videoimage');
  
      video_collection.findOne(
        { refvideo: { $regex: '' + vid + '(.*)' } },
        (err, result) => {
          console.log(err, result);
          if (err) {
            client.close();
            reject(err)
          } else {
            client.close();
            resolve(generateImageUrl(result));
          }  
        }
      );
    });
  });
  
};

module.exports = getImageForVideo;