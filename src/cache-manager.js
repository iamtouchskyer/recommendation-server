var fs      = require('fs');
var md5     = require('md5');

function _path(key) {
  return __dirname + '/cache/' + md5(key);
}

function WriteToCache(key, data) {
  var path = _path(key);

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