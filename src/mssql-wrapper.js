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
  runSqlQuery: async function (statement, cacheable=true) {
    if (cacheMgr.isInCache(statement))
      return await cacheMgr.readFromCache(statement);

    return await new mssql.ConnectionPool(this.config).connect().then(pool => {
      return pool.request().query(statement);
    }).then(result => {
      const rows = result.recordset;

      if (cacheable) {
        if (rows !== null && rows !== undefined) {
          cacheMgr.writeToCache(statement, rows);
        } else {
          console.log(`Error: statement: ${statement}`);
        }
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

  return (
    _.shuffle(
      _.map(result, _.property('hid'))
        .concat([
          '0839CF808BFD5F767B2E8EA3999F396F',
          '040266DE6A4FDE46392FC8F1A166E2D9',
          '0D38581432B0B7709E9C489DE357CA6D',
          '088F3AF3C9295154EA06D499F314F07B',
          '0670A28F6E2AEA9DA90F1628C34E621E',
          '015CB49D777C404CA96CAD311D932743',
          '0528B730CCEE141C254B2D11171D71E8',
          '0CE77593E3C60E8ECC84EDBBB93EB985',
          '0300BE441414F4706619AA9EF92B2280',
          '022B3FCE7582A85C6E00014EDA632342',
          '05CEA27CA8884705AE95B9DCB3E09EFA',
          '04A4E8CBFEAC9AD2D8FD478438ACDA0B',
          '0E122990BDFA9E115423210191CDED9F',
          '0BAC1FF2D49D09C946A5102FAEC924D6',
          '04B0A51A8D2951C362713551B3BE9086',
          '08108E5663E30AA6990683BFA811C034',
          '02E8429F45739B6E67B096224035B0DF',
          '0A16BA163981C815C78064902AD64214',
          '026791093ABCB2207D467B7FCE25709F',
          '0B62CDABC2D53DFE5368158BA014CDAD',
          '05DB6CDBA460D9156597879A82EE15BF',
          '0CD02DC0EFBEF47182505CCD721DF1A5',
          '0CCC91B9FFC63654CB613CEFFAF079CA',
          '0E31C584E7E556A57F99AA0A057E787A',
          '04040A3AFEF031ED67BBA1FEEB836083',
          '04D41F55E9543C2C235240EFA4CF74EA',
          '00213258D72A049F076A6F0BB9C2F210',
          '036DE324AF8AFCA7DF3AC4E150602D90',
          '07BD7F8BD98C94D3AF2934DB8B8D0DD2',
          '0C5EA0DE5EA31011F46799E8AB109789',
          '0E5DC20243A00F528F96181622CE8541',
          '032B35449BB93122F17BFC6170715AC6',
          '056AB4731B3847DC54114F83437F8B20',
          '03BEE20C8001E0D0F08C9DE3FEDAE998',
          '0BCAB1BEF294CF2077F74ADF08E24082',
          '058D643C3276DF08C6E4B565D45A31F6',
          '0B15FC6388EF99607E6B28AC2764FF32',
          '09DD814C7A7D06ECC39DF173729DB4D1',
          '0A9732CE7C528AA0F725F4E1B77722CE',
          '09B8DD780F627EC10ABE5634C7D2959D',
          '0F0D7F68EAB897D86380A7BAA894583F',
      ])
    )
  );
}

async function getUserRecommendationByHid(hid) {
  /*
  const hourMapping = {
    0: '深夜',
    1: '深夜',
    2: '深夜',
    3: '凌晨',
    4: '凌晨',
    5: '凌晨',
    6: '上午',
    7: '上午',
    8: '上午',
    9: '上午',
    10: '上午',
    11: '上午',
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
  */

  const hourMapping = {
    0: 0,
    1: 1,
    2: 2,
    3: 3,
    4: 4,
    5: 5,
    6: 6,
    7: 7,
    8: 8,
    9: 9,
    10: 10,
    11: 11,
    12: 12,
    13: 13,
    14: 14,
    15: 15,
    16: 16,
    17: 17,
    18: 18,
    19: 19,
    20: 20,
    21: 21,
    22: 22,
    23: 23,
  };

  const queryString = `select hour, videolist from dbo.PredictForUsers where hid = '${hid}'`;
  const result = await db.runSqlQuery(queryString);

  const videoListByTimeCategory = _.reduce(result, (memo, res) => {
    res.hour = (res.hour + 8) % 24; // FIXME

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

  const listByTimeCategory = await
    Promise.all(_.map(videoListByTimeCategory, async (videoInSpecificTimeCategory) => {
      const videoStr = videoInSpecificTimeCategory.join(',');
      const queryString3 = `select vid, vname, videotype, taginfo, category, area, director, actor, issueyear from dbo.videoInfo where vid IN (${videoStr})`;
      const list = await db.runSqlQuery(queryString3);

      return list;
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

  if (_.isEmpty(result)) {
    return '';
  }

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

    /*
      FIXME
    if (result.RecordsInDeepNight / totalWatched > 0.2) {
      description += '有深夜观影的习惯, ';
    }
    */

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
      const VarietyInVideoType = result.VarietyInVideoType;

      if ((TVInVideoType+MovieInVideoType) / (totalWatched-result.ChildrenInVideoType) > 0.7) {

        const data = await getTagsByHid(hid);

        const tags = _.chain(data)
          .countUnique()
          .map((value, key) => { return { name: key, value: value + 20 }; })
          .filter(value => value.name != 'tv'
                        && value.name != 'movie'
                        && value.name != 'children'
                        && value.name != 'variety'
                        && value.name != '综艺'
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

        if (VarietyInVideoType / (totalWatched-result.ChildrenInVideoType) > 0.6) {
          description += '综艺控, ';
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
