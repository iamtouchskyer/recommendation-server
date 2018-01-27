var mssql       = require('mssql');
var moment      = require('moment');
var _           = require('lodash');
var cacheMgr    = require('./cache-manager');

_.mixin({
  countUnique: (arr) => {
    const a = {};
    _.each(arr, (element) => { a[element] = a[element] ? a[element] + 1 : 1; });

    return a;
  },
});

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
      'appId': '[dbo].[pivot_total_watched_time_provinceid_appid]',
      'channelId': '[dbo].[pivot_total_watched_time_provinceid_chnid]',
    },

    'countOfWhatchedMedia' : {
      'appId': '[dbo].[pivot_count_of_all_watched_media_provinceid_appid]',
      'channelId': '[dbo].[pivot_count_of_all_watched_media_provinceid_chnid]',
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
    0: '深夜',
    1: '深夜',
    2: '深夜',
    3: '凌晨',
    4: '凌晨',
    5: '凌晨',
    6: '早晨',
    7: '早晨',
    8: '早晨',
    9: '早晨',
    10: '早晨',
    11: '早晨',
    12: '中午',
    13: '中午',
    14: '下午',
    15: '下午',
    16: '下午',
    17: '下午',
    18: '下午',
    19: '下午',
    20: '下午',
    21: '下午',
    22: '下午',
    23: '深夜',
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

  const FIXMEListByTimeCategory = _.filter(listByTimeCategory, (arr) => arr.length > 0);
  
  const finalListByTimeCategory = _.zipObject(_.keys(videoListByTimeCategory), FIXMEListByTimeCategory);

  return {
    fullList: fullList,
    listByTimeCategory: finalListByTimeCategory,
  };
}

async function getUserAggregratedViewHistoryByHid(hid) {
  //const result = await db.doSqlStoreProcedure1('[dbo].[aggregate_user_video_info]', hid);
  let result = await db.runSqlQuery(`select * from [dbo].[aggregate_user_video_info] where hid='${hid}'`);

  const keys = [
    'RecordsInWeekEnd',
    'RecordsInWeekDay',
    'RecordsInDeepNight',
    'TagInfoWithWenYi',
    'TagInfoWithJingSong',
    'TagInfoWithTuiLi',
    'TagInfoWithXuanYi',
    'AreaWithJapan',
    'AreaWithKoera',
    'AreaWithEngland',
    'AreaWithAmerica',
    'TheaterInVideoType',
    'DhyanaInVideoType',
    'MovieInVideoType',
    'TVInVideoType',
    'ChildrenInVideoType',
    'ShoppingInVideoType',
    'AutoInVideoType',
    'SportInVideoType',
    'MangaInVideoType',
    'VarietyInVideoType',
    'GameInVideoType',
    'NewsInVideoType',
  ];

  let tmp = result[0];
  _.each(keys, (key) => {
    tmp[key] = parseInt(tmp[key]) + (result.length > 1 ? parseInt(result[1][key]) : 0);
  });

  result = tmp;

  try {

    let description = '用户';
    const totalWatched = result.RecordsInWeekEnd + result.RecordsInWeekDay;

    if (result.areaname) {
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
        description += '大部分观影时间集中在非工作日, ';
      } else {
        description += '绝大部分观影时间集中在非工作日, ';
      }
    }

    if (result.RecordsInDeepNight / totalWatched > 0.2) {
      description += '有深夜观影的习惯, ';
    }

    if (result.TagInfoWithWenYi || result.TagInfoWithJingSong || result.TagInfoWithTuiLi || result.TagInfoWithXuanYi) {
      const TagInfoWithWenYi = result.TagInfoWithWenYi;
      const TagInfoWithJingSong = (result.TagInfoWithJingSong);
      const TagInfoWithTuiLi = (result.TagInfoWithTuiLi);
      const TagInfoWithXuanYi = (result.TagInfoWithXuanYi);
      if (TagInfoWithWenYi / totalWatched > 0.4) {
        description += '小清新文艺范, ';
      } 
      if ((TagInfoWithJingSong + TagInfoWithTuiLi + TagInfoWithXuanYi) / totalWatched > 1) {
        description += '喜好烧脑，喜欢悬疑惊悚, ';
      }
    }

    if (result.AreaWithJapan || result.AreaWithKoera || result.AreaWithEngland || result.AreaWithAmerica || result.ChildrenInVideoType || result.MangaInVideoType) {
      const ChildrenInVideoType = (result.ChildrenInVideoType);
      const MangaInVideoType = (result.MangaInVideoType);
      const AreaWithAmerica = (result.AreaWithAmerica);
      const AreaWithEngland = (result.AreaWithEngland);
      const AreaWithKoera = (result.AreaWithKoera);
      const AreaWithJapan = (result.AreaWithJapan);
      
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
      const DhyanaInVideoType = result.DhyanaInVideoType;
      if (DhyanaInVideoType > 0) {
        description += '喜爱禅文化, ';
      }
    }

    if (result.TheaterInVideoType) {
      const TheaterInVideoType = result.TheaterInVideoType;
      if (TheaterInVideoType > 0) {
        description += '喜爱戏剧, ';
      }
    }
    
    if (result.GameInVideoType) {
      const GameInVideoType = result.GameInVideoType;
      if (GameInVideoType / totalWatched > 0.3) {
        description += '游戏控, ';
      }
    }

    if (result.SportInVideoType) {
      const SportInVideoType = result.SportInVideoType;
      if (SportInVideoType / totalWatched > 0.3) {
        description += '喜欢体育运动, ';
      }
    }

    if (result.AutoInVideoType) {
      const AutoInVideoType = result.AutoInVideoType;
      if (AutoInVideoType / totalWatched > 0.3) {
        description += '爱车一族, ';
      }
    }

    if (result.ShoppingInVideoType) {
      const ShoppingInVideoType = result.ShoppingInVideoType;
      if (ShoppingInVideoType / totalWatched > 0.2) {
        description += '喜欢购物，爱看电视购物栏目, ';
      }
    }

    if (result.MovieInVideoType || result.TVInVideoType) {
      const MovieInVideoType = result.MovieInVideoType;
      const TVInVideoType = result.TVInVideoType;

      if ((TVInVideoType+MovieInVideoType) / (totalWatched-result.ChildrenInVideoType) > 0.7) {

        const data = await getTagsByHid(hid);

        const tags = _.chain(data)
          .countUnique()
          .map((value, key) => { return { name: key, value: value + 20 }; })
          .filter(value => value.name != 'tv' 
                        && value.name != 'movie' 
                        && value.name != 'children'
                        && value.name != '少儿'
                        && value.name != '电视剧' 
                        && value.name != '电影' 
                        && value.name != '中国大陆')
          .sortBy(element => -element.value)
          .value();

        if (MovieInVideoType / (totalWatched-result.ChildrenInVideoType) > 0.6) {
          description += '电影控, ';
        }

        if (TVInVideoType / (totalWatched-result.ChildrenInVideoType) > 0.6) {
          description += '喜欢看连续剧, ';
        }

        description += `类型集中在 ${tags[0].name}, ${tags[1].name}, ${tags[2].name}, `;
      }
    }

    return description;
  }
  catch (err) {
    console.log(err);
  
    return '';
  }
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
  const queryString = `select vid from dbo.events where hid='${hid}'`;

  const vidList = await db.runSqlQuery(queryString);

  const uniqueVideoLists = _.chain(vidList)
    .map(_.property('vid'))
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
