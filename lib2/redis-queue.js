var q = require('q');
var Backoff = require('./backoff');
var encoder = require('./encoder');

var RedisQueue = function(redisClient, opts) {
  this.name = opts.queue;
  this.workingSet = opts.workingSet;
  this.redisClient = redisClient;
  
  this.taskQueue = 'q:' + this.name;
  this.retryQueue = 'q:' + (opts.retry || this.name);
  
  this.poppers = [];
  
  this.backoff = new Backoff(this._cycle, this);
};

RedisQueue.prototype.push = function(message) {
  if (!message) { return q(); }
  return this.redisClient.lpush(this.taskQueue, message);
};

RedisQueue.prototype.pop = function(id) {
  var d = q.defer();
  
  this.poppers.unshift({
    id: id,
    deferred: d
  });
  this.backoff.immediate();
  
  return d.promise;
};

RedisQueue.prototype.complete = function(message) {
  return this.redisClient.zrem(this.workingSet, message);
};

RedisQueue.prototype.heartbeat = function(message) {
  return this.redisClient.zadd(this.workingSet, Date.now(), message);
};

RedisQueue.prototype._cycle = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var popper = this.poppers.pop();
  
  // prepend with machine/worker id and retry queue
  var prefix = (popper.id || '') + '|' + this.retryQueue + '|';
  
  this.redisClient.scripts.rpopzadd(this.taskQueue, this.workingSet, Date.now(), prefix).then(function(message) {
    if (message) {
      var match = /^([^|]*)\|([^|]+)\|(.*)$/.exec(message);
      var task = encoder.decode(match[3]);
      
      popper.deferred.resolve({
        id: message,
        task: task
      });
      self.backoff.reset();
    } else {
      self.poppers.push(popper);
      self.backoff.next();
    }
  }).catch(function(err) {
    console.log(err.stack);
    self.backoff.next();
  });
};

module.exports = RedisQueue;
