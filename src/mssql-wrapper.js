var mssql       = require('mssql');
var moment      = require('moment');
var _           = require('lodash');
var cacheMgr    = require('./cache-manager');

var db = {
  config: {
    user: 'stcarec',
    password: '1qaz2wsx!',
    server: 'stcarecdb.database.windows.net',
    database: 'stcarec-videoinfo',
    connectionTimeout: 300000,
    requestTimeout: 300000,
    options: {
      encrypt: true // Use this if you're on Windows Azure
    },
  },
  runSqlQuery: async function (statement) {
    if (cacheMgr.isInCache(statement))
      return await cacheMgr.readFromCache(statement);

    return await new mssql.ConnectionPool(this.config).connect().then(pool => {
      return pool.request().query(statement);
    }).then(result => {
      const rows = result.recordset;

      cacheMgr.writeToCache(statement, rows);

      //res.setHeader('Access-Control-Allow-Origin', '*')
      //res.status(200).json(rows);
      mssql.close();
      return rows;
    }).catch(err => {
      //res.status(500).send({ message: "${err}"})
      console.log(err);
      mssql.close();
      return null;
    });
  },
  doSqlStoreProcedure: async function (storedProcedureName, inputDate) {
    const cacheKey = storedProcedureName.concat(JSON.stringify(inputDate));
    if (cacheMgr.isInCache(cacheKey))
      return await cacheMgr.readFromCache(cacheKey);

    return await new mssql.ConnectionPool(this.config).connect().then(pool => {
      return pool.request()
        .input('date', mssql.VarChar(10), inputDate)
        .execute(storedProcedureName);
    }).then(result => {
      const rows = result.recordset;

      cacheMgr.writeToCache(cacheKey, rows);

      //res.setHeader('Access-Control-Allow-Origin', '*')
      //res.status(200).json(rows);
      mssql.close();
      return rows;
    }).catch(err => {
      //res.status(500).send({ message: "${err}"})
      console.log(err);
      mssql.close();
      return null;
    });
  },
};

async function doStoredProcedure(storedProcedureName, inputDate) {
  try {

    if (cacheMgr.isInCache(storedProcedureName.concat(inputDate))) {
      return await cacheMgr.readFromCache(storedProcedureName);
    }

    let pool = await mssql.connect(config);
    let countOfVid = await pool.request()
      .input('date', mssql.VarChar(10), inputDate)
      .execute(storedProcedureName);

    countOfVid = countOfVid.recordset;

    cacheMgr.writeToCache(storedProcedureName, countOfVid);

    mssql.close();

    return countOfVid;
  } catch (error) {
    console.log(error);
  }
}

//db.runSqlQuery('select top(10) vid from dbo.events') .then((a) => console.log(a));
//db.runSqlQuery('select top(11) vid from dbo.events') .then((a) => console.log(a));
//db.doSqlStoreProcedure('[dbo].[pivot_active_clients_provinceid_appid]', '2018-01-01').then((a) => console.log(a));

// 距 1970 年 1 月 1 日之间的毫秒数。
// {startDate: 2017-12-30, length: }

async function getDataByDimensionsAndTimeRangeAndCategory(timeRange, dimension, category) {
  const dimensionStoreProcedureMap = {
    'appId': '[dbo].[pivot_active_clients_provinceid_appid]',
    'channelId': '[dbo].[pivot_active_clients_provinceid_chnid]',
  };

  let results = [];
  for (let i=0; i<timeRange.length; i++) {
    const curDate = moment(timeRange.startDate).add(i, 'days').format('YYYY-MM-DD');

    const result = await db.doSqlStoreProcedure(dimensionStoreProcedureMap[dimension], curDate);
    results.push(_.zipObject(['date', category], [curDate, result]));
  }

  return results;
}

async function getActiveClientsByChannel(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'channelId', 'activeClients');
}

//getActiveClientsByChannel({startDate:  (new Date(moment('20171231').calendar())).getTime(), length: 7}).then(results => console.log(results));

async function getActiveClientsByApp(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'appId', 'activeClients');
}

async function getTotalClientsByChannel(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'channelId', 'totalClients');
}

async function getTotalClientsByApp(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'appId', 'totalClients');
}

async function getTotalWatchedTimeByChannel(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'channelId', 'totalWatchedTime');
}

async function getTotalWatchedTimeByApp(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'appId', 'totalWatchedTime');
}

async function getCountOfWatchedMediaByChannel(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'channelId', 'countOfWhatchedMedia');
}

async function getCountOfWatchedMediaByApp(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'appId', 'countOfWhatchedMedia');
}


async function getUserRecommendation(uid) {
  try {
    const queryString = `select * from dbo.PredictForUsers where hid = ${uid}`;

    if (cacheMgr.isInCache(queryString))
      return await cacheMgr.readFromCache(queryString);

    let pool = await mssql.connect(config);
    let userRecommendation = await pool.request()
      .input(uid, mssql.VarChar(33), uid)
      .query('select * from dbo.PredictForUsers where hid = @uid').recordsets[0];

    cacheMgr.writeToCache(queryString, userRecommendation);
    
    mssql.close();
    return userRecommendation;
  } catch (error) {
    console.log(error);
  }
}

//getUserRecommendation('3').then((a) => console.log(a));


mssql.on('error', err => {
  // ... error handler
});

module.exports = {
  getUserRecommendation,
  getActiveClientsByChannel,
  getActiveClientsByApp,
  getTotalClientsByChannel,
  getTotalClientsByApp,
  getTotalWatchedTimeByChannel,
  getTotalWatchedTimeByApp,
  getCountOfWatchedMediaByChannel,
  getCountOfWatchedMediaByApp,
};
