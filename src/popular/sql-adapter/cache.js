const cacher = require('sequelize-redis-cache');
const redis = require('redis');
const sequelize = require('./raw');

const redis_uri = 'stcarec.redis.cache.windows.net';

const rc = redis.createClient(6380, redis_uri, { auth_pass: 'a9zEDcTYgvuXdy6qwvxm5lJps3YTj1hO2G9bkrwJYjg=', tls: { servername: redis_uri } })

var cacherObj = cacher(sequelize, rc);

module.exports = {
  cacherObj,
  DEFAULT_TTL: 60 * 60 * 24
};

