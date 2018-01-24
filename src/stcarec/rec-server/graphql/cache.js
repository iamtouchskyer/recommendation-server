const DataLoader = require('dataloader');
const { extractUniqueValues, runSproc } = require('../sql-adapter/index');

// loads video info. keys are [(videotype, column)]
const videoInfoLoader = new DataLoader(typeCols => {
  // Need to load all combinations
  var waitingOn = typeCols.length;
  var promises = [];
  
  typeCols.forEach((typeCol, index) => {
    const { videotype, column } = typeCol;

    var p = extractUniqueValues(videotype, column);

    promises.push(p);
  });
  
  return Promise.all(promises);
});

module.exports = {
  videoInfoLoader
};