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

      if (rows !== null && rows !== undefined) {
        cacheMgr.writeToCache(statement, rows);
      } else {
        console.log(`Error: statement: ${statement}`);
      }

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

      if (rows !== null && rows !== undefined) {
        cacheMgr.writeToCache(cacheKey, rows);
      } else {
        console.log(`Error: storeProcedureName: ${storedProcedureName}, date: ${JSON.stringify(inputDate)}`);
      }

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
  doSqlStoreProcedure1: async function (storedProcedureName, hid) {
    const cacheKey = storedProcedureName.concat(JSON.stringify(inputDate));
    if (cacheMgr.isInCache(cacheKey))
      return await cacheMgr.readFromCache(cacheKey);

    return await new mssql.ConnectionPool(this.config).connect().then(pool => {
      return pool.request()
        .input('hid', mssql.VarChar(33), hid)
        .execute(storedProcedureName);
    }).then(result => {
      const rows = result.recordset;

      if (rows !== null && rows !== undefined) {
        cacheMgr.writeToCache(cacheKey, rows);
      } else {
        console.log(`Error: storeProcedureName: ${storedProcedureName}, date: ${JSON.stringify(inputDate)}`);
      }

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
    //  'appId': '[dbo].[pivot_count_of_watched_flattened_media_provinceid_appid]',
    //  'channelId': '[dbo].[pivot_count_of_watched_flattened_media_provinceid_chnid]',
      'appId': '[dbo].[pivot_count_of_all_watched_media_provinceid_appid]',
      'channelId': '[dbo].[pivot_count_of_all_watched_media_provinceid_chnid]',
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

async function getUserListWhoHasRecommendation(size) {
  const queryString = `select distinct top (${size}) hid from dbo.PredictForUsers`;
  const result = await db.runSqlQuery(queryString);

  return (_.map(result, _.property('hid')));
}

async function getUserRecommendationByHid(hid) {
  const hourMapping = {
    0: 'lateNight',
    1: 'lateNight',
    2: 'lateNight',
    3: 'earlyMorning',
    4: 'earlyMorning',
    5: 'earlyMorning',
    6: 'earlyMorning',
    7: 'morning',
    8: 'morning',
    9: 'morning',
    10: 'morning',
    11: 'morning',
    12: 'noon',
    13: 'noon',
    14: 'afternoon',
    15: 'afternoon',
    16: 'afternoon',
    17: 'afternoon',
    18: 'afternoon',
    19: 'night',
    20: 'night',
    21: 'night',
    22: 'night',
    23: 'lateNight',
  };

  const queryString = `select hour, videolist from dbo.PredictForUsers where hid = '${hid}'`;
  const result = await db.runSqlQuery(queryString);

  const videoListByTimeCategory = _.reduce(result, (memo, res) => {
    const key = hourMapping[res.hour];

    if (memo[key]) {
      memo[key] = memo[key].concat(res.videolist.split(';'));
    } else {
      memo[key] = res.videolist.split(';');
    }

    memo[key] = _.uniq(memo[key]);

    return memo;
  }, {});

  const fullVideoLists = _.map(result, (res) => {
    return res.videolist.split(';');
  });

  const uniqueVideoLists = _.chain(fullVideoLists)
    .flatten()
    .uniq()
    .value();

  const queryString2 = `select vid, vname, videotype, taginfo, category, area, director, actor, issueyear from dbo.videoInfo where vid IN (${uniqueVideoLists.join(',')})`;
  const fullList = await db.runSqlQuery(queryString2);

  /* FIXME */
  const FIXMEFullList = _.filter(fullList, (item) => !(item.videotype === 'manga' && item.category.indexOf('亲子') === -1) );
  const listByTimeCategory = await
    Promise.all(_.map(videoListByTimeCategory, async (videoInSpecificTimeCategory) => {
      const videoStr = videoInSpecificTimeCategory.join(',');
      const queryString3 = `select vid, vname, videotype, taginfo, category, area, director, actor, issueyear from dbo.videoInfo where vid IN (${videoStr})`;
      const list = await db.runSqlQuery(queryString3);

      /* FIXME */
      const FIXMEList = _.filter(list, (item) => !(item.videotype === 'manga' && item.category.indexOf('亲子') === -1) );

      return FIXMEList;
    }));
  
  const finalListByTimeCategory = _.zipObject(_.keys(videoListByTimeCategory), listByTimeCategory);

  return {
    fullList: fullList,
    listByTimeCategory: finalListByTimeCategory,
  };
}

async function getUserAggregratedViewHistoryByHid(hid) {
  //const result = await db.doSqlStoreProcedure1('[dbo].[aggregate_user_video_info]', hid);
  let result = await db.runSqlQuery(`select * from [dbo].[aggregate_user_video_info] where hid='${hid}'`);
  if (result.length > 1) {
    result = result[1];
  }

  let description = '用户';
  const totalWatched = parseInt(result.RecordsInWeekEnd, 10) + parseInt(result.RecordsInWeekDay, 10);
  
  if (result.areaTemplate) {
    description += `来自 ${result.areaname}, `;
  }
  if (result.AllDirectors) {

  }
  if (result.AllActors) {

  }
  if (result.RecordsInWeekEnd || result.RecordsInWeekDay) {
    if (result.RecordsInWeekEnd > 0.8) {
      description += '绝大部分观影时间集中在周末, ';
    } else if (result.RecordsInWeekEnd > 0.6) {
      description += '大部分观影时间集中在周末, ';
    } else if (result.RecordsInWeekEnd > 0.4) {
      description += '观影时间分布比较平均';
    } else if (result.RecordsInWeekEnd > 0.2) {
      description += '大部分观影时间集中在平时, ';
    } else {
      description += '绝大部分观影时间集中在平时, ';
    }
  }

  if (result.TagInfoWithWenYi || result.TagInfoWithJingSong || result.TagInfoWithTuiLi || result.TagInfoWithXuanYi) {
    const TagInfoWithWenYi = parseInt(result.TagInfoWithWenYi);
    const TagInfoWithJingSong = parseInt(result.TagInfoWithJingSong);
    const TagInfoWithTuiLi = parseInt(result.TagInfoWithTuiLi);
    const TagInfoWithXuanYi = parseInt(result.TagInfoWithXuanYi);
    if (TagInfoWithWenYi / totalWatched > 0.4) {
      description += '小清新文艺范, ';
    } 
    if ((TagInfoWithJingSong + TagInfoWithTuiLi + TagInfoWithXuanYi) / totalWatched > 0.4) {
      description += '喜好烧脑，喜欢悬疑惊悚, ';
    }
  }

  if (result.AreaWithJapan || result.AreaWithKoera || result.AreaWithEngland || result.AreaWithAmerica || result.ChildrenInVideoType || result.MangaInVideoType) {
    const ChildrenInVideoType = parseInt(result.ChildrenInVideoType);
    const MangaInVideoType = parseInt(result.MangaInVideoType);
    const AreaWithAmerica = parseInt(result.AreaWithAmerica);
    const AreaWithEngland = parseInt(result.AreaWithEngland);
    const AreaWithKoera = parseInt(result.AreaWithKoera);
    const AreaWithJapan = parseInt(result.AreaWithJapan);
    
    const foreignAreaTotal = AreaWithAmerica + AreaWithKoera + AreaWithEngland + AreaWithJapan;
    const childrenTotal = ChildrenInVideoType + MangaInVideoType;

    if (childrenTotal / totalWatched > 0.1) {
      description += '家有小宝, ';

      if (childrenTotal / totalWatched > 0.7) {
        description += '基本拿来为孩子服务, ';
      }
    }

    if (childrenTotal / totalWatched < 0.2) {
      if ((AreaWithEngland + AreaWithAmerica) / totalWatched > 0.5) {
        description += '喜欢美剧英剧, ';
      } 
      if ((AreaWithKoera + AreaWithJapan) / totalWatched > 0.5) {
        description += '喜欢日剧韩剧, ';
      } 
    }

    if (foreignAreaTotal / totalWatched < 0.2) {
      description += '对国外影片综艺兴趣不大, ';
    }
  }

  if (result.DhyanaInVideoType) {
    const DhyanaInVideoType = parseInt(DhyanaInVideoType, 10);
    if (DhyanaInVideoType > 0) {
      description += '喜爱禅文化, ';
    }
  }

  if (result.TheaterInVideoType) {
    const TheaterInVideoType = parseInt(TheaterInVideoType, 10);
    if (TheaterInVideoType > 0) {
      description += '喜爱戏剧, ';
    }
  }
  
  if (result.GameInVideoType) {
    const GameInVideoType = parseInt(result.GameInVideoType, 10);
    if (GameInVideoType / totalWatched > 0.3) {
      description += '游戏控';
    }
  }

  if (result.SportInVideoType) {
    const SportInVideoType = parseInt(result.SportInVideoType, 10);
    if (SportInVideoType / totalWatched > 0.3) {
      description += '喜欢体育运动';
    }
  }

  if (result.AutoInVideoType) {
    const AutoInVideoType = parseInt(result.AutoInVideoType, 10);
    if (AutoInVideoType / totalWatched > 0.3) {
      description += '爱车一族';
    }
  }

  if (result.ShoppingInVideoType) {
    const ShoppingInVideoType = parseInt(result.ShoppingInVideoType, 10);
    if (ShoppingInVideoType / totalWatched > 0.2) {
      description += '喜欢购物，爱看电视购物栏目';
    }
  }

  if (result.MovieInVideoType) {
    const MovieInVideoType = parseInt(result.MovieInVideoType, 10);
    if (MovieInVideoType / totalWatched > 0.7) {
      description += '电影控';
    }
  }

  if (result.TVInVideoType) {
    const TVInVideoType = parseInt(result.TVInVideoType, 10);
    if (TVInVideoType / totalWatched > 0.7) {
      description += '综艺控';
    }
  }

  return description;
}

async function getTagsByHid(hid) {
  const queryString = `select distinct vid as dvid from dbo.events where hid='${hid}'`;

  const vidList = await db.runSqlQuery(queryString);

  const uniqueVideoLists = _.chain(vidList)
    .map(_.property('dvid'))
    .filter((vid) => vid != '0')
    .value();

  const queryString2 = `select vid, vname, videotype, taginfo, category, area, director, actor, issueyear from dbo.videoInfo where vid IN (${uniqueVideoLists.join(',')})`;
  const result2 = await db.runSqlQuery(queryString2);

  const tags = _.chain(result2)
    .map(res => {
      const arr = _.filter([res.videotype, res.taginfo, res.category, res.area, res.director, res.actor].join('|').split('|'), (item) => item);

      return arr;
    })
    .flatten()
    .value();


  return tags;
}

async function getViewHistoryByHid(hid) {
  const queryString = `select distinct vid as dvid from dbo.events where hid='${hid}'`;

  const vidList = await db.runSqlQuery(queryString);

  const uniqueVideoLists = _.chain(vidList)
    .map(_.property('dvid'))
    .filter((vid) => vid != '0')
    .value();

  const queryString2 = `select vid, vname, videotype, taginfo, category, area, director, actor, issueyear from dbo.videoInfo where vid IN (${uniqueVideoLists.join(',')})`;
  const result2 = await db.runSqlQuery(queryString2);

  return result2;
}

mssql.on('error', err => {
  console.log(err);
});

module.exports = {
  getUserListWhoHasRecommendation,
  getUserRecommendationByHid,
  getViewHistoryByHid,
  getTagsByHid,
  getUserAggregratedViewHistoryByHid,
  getActiveClientsByChannel,
  getActiveClientsByApp,
  getNewClientsByChannel,
  getNewClientsByApp,
  getTotalWatchedTimeByChannel,
  getTotalWatchedTimeByApp,
  getCountOfWatchedMediaByChannel,
  getCountOfWatchedMediaByApp,
};
