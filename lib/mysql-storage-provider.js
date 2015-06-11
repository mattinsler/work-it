var MysqlStorage = require('./mysql-storage');
var MysqlStorageAdmin = require('./mysql-storage-admin');

module.exports = function(configuration, config) {
  if (!config) { config = {}; }
  // if (!config.url) { throw new Error('You must configure a MongodbStorageProvider with a url'); }
  // if (!config.collection) { throw new Error('You must configure a MongodbStorageProvider with a collection'); }
  
  var mysql = require('mysql');
  
  var connection = mysql.createConnection(config);
  
  connection.connect();
  
  return {
    get: function(task, execution) {
      return new MysqlStorage(connection, {
        task: task,
        execution: execution
      });
    },
    admin: function() {
      return new MysqlStorageAdmin(connection);
    }
  };
};
