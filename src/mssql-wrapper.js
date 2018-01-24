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

//db.runSqlQuery('select top(10) vid from dbo.events') .then((a) => console.log(a));
//db.runSqlQuery('select top(11) vid from dbo.events') .then((a) => console.log(a));
//db.doSqlStoreProcedure('[dbo].[pivot_active_clients_provinceid_appid]', '2018-01-01').then((a) => console.log(a));

// 距 1970 年 1 月 1 日之间的毫秒数。
// {startDate: 2017-12-30, length: }

async function getDataByDimensionsAndTimeRangeAndCategory(timeRange, dimension, category) {
  const dimensionStoreProcedureMap = {
    'activeClients' : {
      'appId': '[dbo].[pivot_active_clients_provinceid_appid]',
      'channelId': '[dbo].[pivot_active_clients_provinceid_chnid]',
    },

    'newClients' : {
      'appId': '[dbo].[pivot_new_clients_provinceid_appid]',
      'channelId': '[dbo].[pivot_new_clients_provinceid_chnid]',
    },

    'totalWatchedTime' : {
      'appId': '[dbo].[pivot_total_watched_time_provinceid_appid]',
      'channelId': '[dbo].[pivot_total_watched_time_provinceid_chnid]',
    },

    'countOfWhatchedMedia' : {
      'appId': '[dbo].[pivot_count_of_watched_media_provinceid_appid]',
      'channelId': '[dbo].[pivot_count_of_watched_media_provinceid_chnid]',
    },
  };

  let results = [];
  for (let i=0; i<timeRange.length; i++) {
    const curDate = moment(timeRange.startDate).add(i, 'days').format('YYYY-MM-DD');

    const result = await db.doSqlStoreProcedure(dimensionStoreProcedureMap[category][dimension], curDate);
    results.push(_.zipObject(['date', 'data'], [curDate, result]));
  }

  return results;
}

async function getActiveClientsByChannel(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'channelId', 'activeClients');
}

async function getActiveClientsByApp(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'appId', 'activeClients');
}

async function getNewClientsByChannel(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'channelId', 'newClients');
}

async function getNewClientsByApp(timeRange) {
  return await getDataByDimensionsAndTimeRangeAndCategory(timeRange, 'appId', 'newClients');
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

async function getUserRecommendationByHid(hid) {
  const queryString = `select * from dbo.PredictForUsers where hid = '${hid}'`;
  const result = await db.runSqlQuery(queryString);
  console.log(result);

  return result;
}

async function getViewHistoryByHid(hid) {
  const queryString = `select distinct vid as dvid from dbo.events where hid='${hid}'`;

  console.log(queryString);
  const vidList = await db.runSqlQuery(queryString);

  console.log(vidList);

  return vidList;
}

mssql.on('error', err => {
  console.log(err);
});

module.exports = {
  getUserRecommendationByHid,
  getViewHistoryByHid,
  getActiveClientsByChannel,
  getActiveClientsByApp,
  getNewClientsByChannel,
  getNewClientsByApp,
  getTotalWatchedTimeByChannel,
  getTotalWatchedTimeByApp,
  getCountOfWatchedMediaByChannel,
  getCountOfWatchedMediaByApp,
};
