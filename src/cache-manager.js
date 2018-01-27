var fs      = require('fs');
var md5     = require('md5');

function _path(key) {
  const dir = __dirname + '/cache/';

  if (!fs.existsSync(dir)){
    fs.mkdirSync(dir);
  }

  return dir + md5(key);
}

function WriteToCache(key, data) {
  if (data === undefined || data === null) return;

  var path = _path(key);

  console.log('WriteToCache'.concat(path));

  fs.writeFile(path, JSON.stringify(data), function (error) {
    if (error) {
      console.error('write error:  ' + error.message);
    } else {
      console.log('Successful Write to ' + path);
    }
  });
}

function ReadFromCache(key) {
  var path = _path(key);

  console.log('ReadFromCache:'.concat(path));

  return JSON.parse(fs.readFileSync(path));
}

function IsInCache(key) {
  var path = _path(key);

  return fs.existsSync(path);
}

module.exports = {
  writeToCache : WriteToCache,
  readFromCache : ReadFromCache,
  isInCache : IsInCache,
};