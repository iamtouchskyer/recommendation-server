const sequelize = require('./raw');
const VideoInfo = require('./videoinfo');

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
          v.get(column),
          splitter
        )
      )
    );
	})

  return filtered;
}

// Get all filters for certain videoType
const extractUniqueValues = (videoType, column, useCache = false) => {
  let params = {
    attributes:
      [column],
    where: {
      videotype: videoType
    },
    group: column,
    order: [[sequelize.fn('COUNT', sequelize.col(column)), 'DESC']]
  };

  if (useCache === true) {
    let VideoInfoCached = cacherObj.model('videoinfo');
    return VideoInfoCached.findAll(params)
      .then(values => values)  
      .then((values) => {
        console.log(values);
        return splitValues(column, values, /[\s|]/g)
      })
  } else {
    return VideoInfo
      .findAll(params)
      .then(values => splitValues(column, values, /[\s|]/g));
  }
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
  extractUniqueValues,
  runSproc
};
