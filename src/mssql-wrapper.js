var mssql       = require('mssql');
var _           = require('lodash');
var cacheMgr    = require('./cache-manager');

const config = {
  user: 'stcarec',
  password: '1qaz2wsx!',
  server: 'stcarecdb.database.windows.net',
  database: 'stcarec-videoinfo',
  options: {
    encrypt: true // Use this if you're on Windows Azure
  },
};

function init() {
  return mssql.connect(config);
}

async function getTablesBasicInfo(tbName) {
  try {
    const queryString = 'select top(10) count(vid) as CountOfVid from dbo.events group by vid';

    if (cacheMgr.isInCache(queryString))
      return await cacheMgr.readFromCache(queryString);

    let pool = await mssql.connect(config);
    let countOfVid = await pool.request().query(queryString).recordsets[0];

    cacheMgr.writeToCache(queryString, countOfVid);

    return countOfVid;
  } catch (error) {
    console.log(error);
  }
}

getTablesBasicInfo('dbo.events') .then((a) => console.log(a));


/*
mssql.connect(config).then(pool => {
    // Query
    
    return pool.request()
    .input('input_parameter', mssql.Int, value)
    .query('select * from mytable where id = @input_parameter')
}).then(result => {
    console.dir(result)
    
    // Stored procedure
    
    return pool.request()
    .input('input_parameter', mssql.Int, value)
    .output('output_parameter', mssql.VarChar(50))
    .execute('procedure_name')
}).then(result => {
    console.dir(result)
}).catch(err => {
    // ... error checks
})
*/

mssql.on('error', err => {
  // ... error handler
});
