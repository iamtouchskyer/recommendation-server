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


function getBrandList() { return getList('tbl_brand'); }
function getCityList() { return getList('tbl_city'); }
function getBranchList() { return getList('tbl_branch'); }
function getBranchBrandList() { return getList('tbl_branch_brand'); }

const endPoints = [
  {url: '/city', tableKey: 'tbl_city'},
  {url: '/branch', tableKey: 'tbl_branch'},
  {url: '/brand', data: 'tbl_brand'},
  {url: '/branchbrand', tableKey: 'tbl_branch_brand'},

];

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


class DataTable {
  constructor(tableKey) {
    this.tableKey = tableKey;
  }

  async find(params) {
    return mysqlWrapper.getList(this.tableKey);

    /*
      _.chain(BranchData)
       .map(branchItem => {
         var item = _.pick(branchItem, ['branch_id', 'branch_name', 'city_id', 'address', 'status', 'branch_type', 'phone']);

   //      var cityName = _.find(CityData, (cityData) => cityData.parent_id == item.city_id);
   //      item['city_name'] = (cityName && cityName['city_name']) || '-';
         item['city_name'] = item.address.slice(0, 2);

         return item;
        })
        .value()
  },
  */
  }

  async get(id, params) {
    return this.data[parseInt(id, 10)];
  }

  async create(data, params) {}
  async patch(data, params) {}
  async remove(data, params) {}
}

class Tables{
  constructor() {
  }

  async find(params) {
    return mysqlWrapper.getTablesBasicInfo();
  }

  async get(id, params) {
  }

  async create(data, params) {}
  async patch(data, params) {}
  async remove(data, params) {}
}

_.each(endPoints, (endPoint) => app.use(endPoint.url, new DataTable(endPoint.tableKey)));
app.use('/tables', new Tables());

app.set('host', 'localhost');
app.set('port', 3030);


// Register a nicer error handler than the default Express one
app.use(express.errorHandler());

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