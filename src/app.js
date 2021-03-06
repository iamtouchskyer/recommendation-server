const path = require('path');
const favicon = require('serve-favicon');
const compress = require('compression');
const cors = require('cors');
const helmet = require('helmet');
const logger = require('winston');

const feathers = require('@feathersjs/feathers');
const configuration = require('@feathersjs/configuration');
const express = require('@feathersjs/express');
const socketio = require('@feathersjs/socketio');


const middleware = require('./middleware');
const services = require('./services');
const appHooks = require('./app.hooks');
const channels = require('./channels');
const memory = require('feathers-memory');
const _ = require('lodash');
const mysqlWrapper = require('./mysql-wrapper');
const mssqlWrapper = require('./mssql-wrapper');
const moment = require('moment');

const app = express(feathers());

const graphqlHTTP = require('express-graphql');
const root = require('./graphql/root');
const schema = require('./graphql/schema');

const getImageForVideo = require('./services/image');

// This enables CORS
app.use(function(req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');

  if (req.method === 'OPTIONS') {
    res.sendStatus(200);
  } else {
    next();
  }
});

// Parse HTTP JSON bodies
app.use(express.json());
// Parse URL-encoded params
app.use(express.urlencoded({ extended: true }));
// Add REST API support
app.configure(express.rest());
// Configure Socket.io real-time APIs
app.configure(socketio());

const provinces = {
  200: '安徽',
  201: '澳门',
  202: '北京',
  203: '重庆',
  204: '福建',
  205: '甘肃',
  206: '广东',
  207: '广西',
  208: '贵州',
  209: '海南',
  210: '河北',
  211: '河南',
  212: '黑龙江',
  213: '湖北',
  214: '湖南',
  215: '吉林',
  216: '江苏',
  217: '江西',
  218: '辽宁',
  219: '内蒙古',
  220: '宁夏',
  221: '青海',
  222: '山东',
  223: '山西',
  224: '陕西',
  225: '上海',
  226: '四川',
  227: '台湾',
  228: '天津',
  229: '西藏',
  230: '香港',
  231: '新疆',
  232: '云南',
  233: '浙江'
};

class UserOperationData {
  async find(params) {
    try {
    const length = 24;
    const timeRange = { startDate: (new Date(moment('20171231').calendar())).getTime(), length: length };

    const [
      activeClientsByApp, activeClientsByChannel,
      newClientsByApp, newClientsByChannel,
      totalWatchedTimeByApp, totalWatchedTimeByChannel,
      countOfWatchedMediaByApp, countOfWatchedMediaByChannel,
    ] = await Promise.all([
      mssqlWrapper.getActiveClientsByApp(timeRange),
      mssqlWrapper.getActiveClientsByChannel(timeRange),
      mssqlWrapper.getNewClientsByApp(timeRange),
      mssqlWrapper.getNewClientsByChannel(timeRange),
      mssqlWrapper.getTotalWatchedTimeByApp(timeRange),
      mssqlWrapper.getTotalWatchedTimeByChannel(timeRange),
      mssqlWrapper.getCountOfWatchedMediaByApp(timeRange),
      mssqlWrapper.getCountOfWatchedMediaByChannel(timeRange),
    ]);

    let appIds = _.keys(activeClientsByApp[0].data[0]); appIds.splice(-2, 2);
    let channelIds = _.keys(activeClientsByChannel[0].data[0]); channelIds.splice(-2, 2);

    const genData = (dayOfDataByApp, dayOfDataByChannel) => {
      let provincesDataList = [];
      for (let j=0; j<_.size(dayOfDataByApp.data); j++) {
        const clientsByApp = dayOfDataByApp.data;
        const clientsByChannel = dayOfDataByChannel.data;

        let key1 = _.keys(clientsByApp[j]);
        let value1 = _.values(clientsByApp[j]);
        let key2 = _.keys(clientsByChannel[j]);
        let value2 = _.values(clientsByChannel[j]);
        key1.splice(-2, 2);
        key2.splice(-2, 2);
        value1.splice(-2, 2);
        value2.splice(-2, 2);

        let apps = [];
        for (let ii=0; ii<_.size(key1); ++ii) {
          apps.push(_.zipObject(['appId', 'total'], [key1[ii], (value1[ii] === null || value1[ii] === undefined) ? 0 : parseInt(value1[ii])]));
        }
        let channels = [];
        for (let ii=0; ii<_.size(key2); ++ii) {
          channels.push(_.zipObject(['channelId', 'total'], [key2[ii], (value2[ii] === null || value2[ii] === undefined) ? 0 : parseInt(value2[ii])]));
        }

        provincesDataList.push({
          provinceId: clientsByApp[j].provinceid,
          provinceName: clientsByApp[j].provinceid === 0 ? '全国' : provinces[clientsByApp[j].provinceid],
          dimensions: {
            application: apps,
            channel: channels,
          }
        });
      }
      return provincesDataList;
    };

    const data = [];
    for (let i=0; i<length; i++) {
      data.push({
        date: activeClientsByApp[i].date,
        categories: {
          newClients: genData(newClientsByApp[i], newClientsByChannel[i]),
          activeClients: genData(activeClientsByApp[i], activeClientsByChannel[i]),
          totalWatchedTime: genData(totalWatchedTimeByApp[i], totalWatchedTimeByChannel[i]),
          countOfWhatchedMedia: genData(countOfWatchedMediaByApp[i], countOfWatchedMediaByChannel[i]),
        }
      });
    }

    return {
      name: 'operationData',
      data: data,
    };

  } catch (err) {

    console.log(err);
    return {
      name: 'operationData',
      data: {},
    };
  }
  }

  async get(id, params) { }
  async create(data, params) {}
  async patch(data, params) {}
  async remove(data, params) {}
}

class UserList {
  async find(params) {
    const list = mssqlWrapper.getUserListWhoHasRecommendation(100);

    return list;
  }

  async get(id, params) {
    const hid = id;
    const action = params.query.action;

    if (action === 'recommendation') {
      const userRecommendation = await mssqlWrapper.getUserRecommendationByHid(hid);

      return {
        name: 'recommendation',
        data: userRecommendation,
      };
    } else if (action === 'tag') {
      const userTags = await mssqlWrapper.getTagsByHid(id);

      return {
        name: 'tags',
        data: userTags,
      };
    } else if (action === 'history') {
      const userViewHistory = await mssqlWrapper.getViewHistoryByHid(id);

      return {
        name: 'history',
        data: userViewHistory,
      };
    } else if (action === 'summary') {
      const userSummary = await mssqlWrapper.getUserAggregratedViewHistoryByHid(id);

      return {
        name: 'summary',
        data: userSummary,
      };
    } else {
      return {};
    }
  }

  async create(data, params) {}
  async patch(data, params) {}
  async remove(data, params) {}
}


app.use('/api/cibn/operationdata', new UserOperationData());
app.use('/api/cibn/users', new UserList());

/*
  Graph QL
*/
app.use('/api/graphql', graphqlHTTP({
  schema: schema,
  rootValue: root,
  graphiql: true
}));

app.use('/api/cibn/image', (req, res) => {
  let vid = req.query.vid;

  if (vid) {
    getImageForVideo(vid)
      .then((result) => {
        if (result) {
          res.redirect(result);
        } else {
          res.sendStatus(404);
        }  
      })
      .catch((err) => {
        res.sendStatus(500);
      });
  } else {
    res.sendStatus(404);
  }  
});

app.set('host', 'localhost');
app.set('port', 3030);


// Register a nicer error handler than the default Express one
app.use(express.errorHandler( logger ));

// Add any new real-time connection to the `everybody` channel
app.on('connection', connection => app.channel('everybody').join(connection));
// Publish all events to the `everybody` channel
app.publish(data => app.channel('everybody'));

// Start the server
app.listen(app.port).on('listening', () =>
  console.log('Feathers server listening on http://%s:%d', app.get('host'), app.get('port'))
);

module.exports = app;


/*
// Load app configuration
app.configure(configuration());
// Enable CORS, security, compression, favicon and body parsing
app.use(cors());
app.use(helmet());
app.use(compress());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(favicon(path.join(app.get('public'), 'favicon.ico')));
// Host the public folder
app.use('/', express.static(app.get('public')));

// Set up Plugins and providers
app.configure(express.rest());
app.configure(socketio());

// Configure other middleware (see `middleware/index.js`)
app.configure(middleware);
// Set up our services (see `services/index.js`)
app.configure(services);
// Set up event channels (see channels.js)
app.configure(channels);

// Configure a middleware for 404s and the error handler
app.use(express.notFound());
app.use(express.errorHandler({ logger }));

app.hooks(appHooks);
*/
