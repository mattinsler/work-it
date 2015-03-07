var MongodbTaskStatusStorage = function(provider, taskId, executionId) {
  this.taskId = taskId;
  this.executionId = executionId;
  this.model = provider.model;
};

MongodbTaskStatusStorage.prototype.start = function(statusUpdate) {
  var id = this.taskId + ':' + this.executionId;
  
  statusUpdate.task = this.taskId,
  statusUpdate.execution = this.executionId;
  
  return this.model.where({_id: id}).update({$set: statusUpdate}, {upsert: true});
};

MongodbTaskStatusStorage.prototype.finish = function(statusUpdate) {
  var id = this.taskId + ':' + this.executionId;
  return this.model.where({_id: id}).update({$set: statusUpdate}, {upsert: true});
};



var MongodbTaskStatusStorageProvider = function(opts) {
  if (!(this instanceof MongodbTaskStatusStorageProvider)) {
    return new MongodbTaskStatusStorageProvider(opts);
  }
  
  if (!opts) { opts = {}; }
  if (!opts.url) { throw new Error('You must configure a MongodbTaskStatusStorageProvider with a url'); }
  if (!opts.collection) { throw new Error('You must configure a MongodbTaskStatusStorageProvider with a collection'); }
  
  this.url = opts.url;
  this.collection = opts.collection;
  
  var AccessMongo = require('access-mongo');
  AccessMongo.configure({lazy: true});
  AccessMongo.connect(this.url);
  this.model = AccessMongo.createModel(this.collection);
};

MongodbTaskStatusStorageProvider.prototype.get = function(taskId, executionId) {
  return new MongodbTaskStatusStorage(this, taskId, executionId);
};

module.exports = MongodbTaskStatusStorageProvider;
