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

// This enables CORS
app.use(function(req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// Parse HTTP JSON bodies
app.use(express.json());
// Parse URL-encoded params
app.use(express.urlencoded({ extended: true }));
// Add REST API support
app.configure(express.rest());
// Configure Socket.io real-time APIs
app.configure(socketio());

const provinces = ['安徽', '澳门', '北京', '重庆', '福建', '甘肃', '广东', '广西', '贵州', '海南', '河北', '黑龙江', '河南', '湖北', '湖南', '江苏', '江西', '吉林', '辽宁省', '内蒙古', '宁夏', '青海', '山东', '上海', '陕西', '山西', '四川', '台湾', '天津', '香港', '新疆', '西藏', '云南', '浙江'];

class UserOperationData {
  async find(params) {
    const timeRange = { startDate: (new Date(moment('20171231').calendar())).getTime(), length: 7 };

    const [activeClientsByApp, activeClientsChannel] = await Promise.all([
      mssqlWrapper.getActiveClientsByApp(timeRange),
      mssqlWrapper.getActiveClientsByChannel(timeRange),
    ]);

    const data = [];
    for (let i=0; i<_.size(activeClientsByApp); i++) {
      let provincesDataList = [];
      for (let j=0; j<_.size(activeClientsByApp[i].activeClients); j++) {
        const clients = activeClientsByApp[i].activeClients;
        if (clients[j].provinceid === 0) continue;

        provincesDataList.push({
          provinceId: clients[j].provinceid,
          provinceName: provinces[clients[j].provinceid-200],
          dimensions: {
            application: [
              {appId: 1000, total: clients[j]['1000']},
              {appId: 1005, total: clients[j]['1005']},
              {appId: 1008, total: clients[j]['1008']},
              {appId: 1031, total: clients[j]['1031']},
            ],
            channel: [
              {appId: 1000, total: clients[j]['1000']},
              {appId: 1005, total: clients[j]['1005']},
              {appId: 1008, total: clients[j]['1008']},
              {appId: 1031, total: clients[j]['1031']},
            ],
          }
        });
      }

      data.push({
        date: activeClientsByApp[i].date,
        categories: {
          newClients: provincesDataList,
          activeClients: provincesDataList,
          totalWatchedTime: provincesDataList,
          countOfWhatchedMedia: provincesDataList,
        }
      });
    }

    console.log(data);

    return {
      name: 'operationData',
      data: data,
    };
  }

  async get(id, params) { }
  async create(data, params) {}
  async patch(data, params) {}
  async remove(data, params) {}
}

class Test {
  async find(params) {
    console.log(params);

    return {
      name: 'operationData',
    };
  }

  async get(id, params) { }
  async create(data, params) {}
  async patch(data, params) {}
  async remove(data, params) {}
}

const a = new UserOperationData();
app.use('/useroperation', a);
app.use('/api/cibn/operationdata', a);
app.use('/test', new Test());

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