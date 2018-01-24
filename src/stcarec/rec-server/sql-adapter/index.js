const sequelize = require('./raw');
const VideoInfo = require('./videoinfo');

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
const extractUniqueValues = (videoType, column) => {
  return VideoInfo
    .findAll({
      attributes: 
        [column],
      where: {
        videotype: videoType
      },
      group: column,
      order: [[sequelize.fn('COUNT', sequelize.col(column)), 'DESC']]
    })
    .then(values => splitValues(column, values, /[\s|]/g));
}

// Call a stored procedure on server, with procParams
// procParams = {
//      @param1 = value1   
// }
const runSproc = (procName, procParams) => {
  let params = _.join(
    _.transform(procParams, (result, value, key) => {
      return result.push('@' + key + '=' + value)
    }, []),
    ','
  );
  return sequelize.query('EXEC ' + procName + ' ' + params + ';');
}

module.exports = {
  extractUniqueValues,
  runSproc
};
