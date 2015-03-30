var MongodbStorage = function(mongodbClient, opts) {
  this.mongodbClient = mongodbClient;
  this.task = opts.task;
  this.execution = opts.execution;
  
  this._id = this.task + ':' + this.execution;
};

MongodbStorage.prototype.update = function(update) {
  return this.mongodbClient.where({_id: this._id}).update({$set: update}, {upsert: true});
}

MongodbStorage.prototype.start = function(update) {
  update.task = this.task,
  update.execution = this.execution;
  return this.update(update);
};

MongodbStorage.prototype.finish = function(update) {
  return this.update(update);
};

module.exports = MongodbStorage;
