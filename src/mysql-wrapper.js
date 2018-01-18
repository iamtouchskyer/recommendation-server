var mysql      = require('promise-mysql');
var _           = require('lodash');

function init(db) {
  return mysql.createConnection({
    host     : 'localhost',
    user     : 'root',
    password : 'qwert_12345',
    database : db
  });
}

function destroy(conn) {
  conn.end();
}

function getList(tableName) {
  return init('pindou')
          .then((conn) => {
              var result = conn.query(`SELECT * FROM ${tableName}`);
              destroy(conn);
              return result;
            });
}

function getTablesBasicInfo() {
  var connection;
  return init('information_schema')
          .then((conn) => {
            connection = conn;
            return conn.query("select data_length AS Size, table_rows AS NumOfRows, table_name AS Name from tables where table_schema = 'pindou'");
          }).catch((error) => {
            console.log(error);
          });
}

module.exports = {
  getList: getList,
  getTablesBasicInfo: getTablesBasicInfo,
};


/*

          .then((tableList) => {
            console.log(tableList);
              var result = 
                Promise.all(_.map(tableList, (tableItemName) => connection.query(`
                    SELECT 
                      CONCAT(TRUNCATE(SUM(data_length)/1024/1024,2),'MB') AS Size,  
                      table_rows AS NumOfRows,
                      table_name AS Name,
                      FROM tables WHERE TABLE_NAME = "${tableItemName.table_name}"`
                    )
                  )
                );
              destroy(connection);
              return result;
            }
*/