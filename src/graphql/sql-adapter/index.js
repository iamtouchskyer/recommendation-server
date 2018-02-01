const sequelize = require('./raw');
const VideoInfo = require('./models/videoinfo');

const { cacherObj, DEFAULT_TTL } = require('./cache');

const _ = require('lodash');

const splitValues = (column, values, splitter) => {
	let filtered = [];

  values.forEach((v) => {
    filtered = _.compact(
      _.union(
        filtered,
        // Split each value by `splitter`
        _.split(
          _.has(v, 'get') ? v.get(column) : v[column],
          splitter
        )
      )
    );
	})

  return filtered;
}

// Get all filters for certain videoType
const extractUniqueValuesInVideoType = (videoType, column, useCache = false) => {
  let params = {
    attributes:
      [column],
    where: {
      videotype: videoType
    },
    group: column,
    order: [[sequelize.fn('COUNT', sequelize.col(column)), 'DESC']]
  };

  let model = VideoInfo.findAll(params);

  if (useCache === true) {
    mode = cacherObj
      .model('videoinfo').ttl(DEFAULT_TTL);
  }

  return model
    .then(values => splitValues(column, values, /[\s|]/g));
}

// Call a stored procedure on server, with procParams
// procParams = {
//      @param1 = value1   
// }
const runSproc = (procName, procParams, useCache = false) => {
  let params = _.join(
    _.transform(procParams, (result, value, key) => {
      return result.push('@' + key + '=' + value)
    }, []),
    ','
  );

  let db = sequelize;

  if (useCache === true) {
    db = cacherObj.ttl(DEFAULT_TTL);
  }
  return db.query('EXEC ' + procName + ' ' + params + ';', {type: sequelize.QueryTypes.SELECT});
}

module.exports = {
  extractUniqueValuesInVideoType,
  runSproc
};
