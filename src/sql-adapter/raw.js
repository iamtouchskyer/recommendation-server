const Sequelize = require('sequelize');

const connect = () => {
  const connection = new Sequelize('stcarec-videoinfo', 'stcarec', '1qaz2wsx!', {
    host: 'stcarecdb.database.windows.net',
    dialect: 'mssql',
    
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
  
    dialectOptions: {
      encrypt: true,
      requestTimeout: 1000 * 60 * 30,
    }
  });

  return connection;
}

const sequelize = connect();

module.exports = sequelize;
