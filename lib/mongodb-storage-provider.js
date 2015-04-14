var MongodbStorage = require('./mongodb-storage');

module.exports = function(configuration, config) {
  if (!config) { config = {}; }
  if (!config.url) { throw new Error('You must configure a MongodbStorageProvider with a url'); }
  if (!config.collection) { throw new Error('You must configure a MongodbStorageProvider with a collection'); }
  
  var AccessMongo = require('access-mongo');
  AccessMongo.configure({lazy: true});
  AccessMongo.setWriteConcern({j: true});
  AccessMongo.connect(config.url);
  
  var mongodbClient = AccessMongo.createModel(config.collection);
  
  return {
    get: function(task, execution) {
      return new MongodbStorage(mongodbClient, {
        task: task,
        execution: execution
      });
    }
  };
};
