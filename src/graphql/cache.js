const DataLoader = require('dataloader');
const {
  extractUniqueValuesInVideoType,
  runSproc
} = require('./sql-adapter/index');

// loads video info. keys are [(videotype, column)]
const videoInfoLoader = new DataLoader(typeCols => {
  // Need to load all combinations
  var waitingOn = typeCols.length;
  var promises = [];
  
  typeCols.forEach((typeCol, index) => {
    const { videotype, column } = typeCol;

    // somehow caching for this doesn't work.
    var p = extractUniqueValuesInVideoType(videotype, column, true);

    promises.push(p);
  });
  
  return Promise.all(promises);
});

module.exports = {
  videoInfoLoader
};