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
  
  var encodedMessage = encoder.encodeMessage({
    failedCount: 0,
    reapedCount: 0,
    task: message
  });
  
  return this.redisClient.lpush(this.taskQueue, encodedMessage).then(function() {
    return encodedMessage;
  });
};

RedisQueue.prototype.adopt = function(message, opts) {
  var encodedMessage = encoder.encodeMessage({
    popper: opts.popper || '',
    retryQueue: opts.retry || this.retryQueue,
    failedCount: 0,
    reapedCount: 0,
    task: message
  });
  
  return this.redisClient.zadd(this.workingSet, Date.now(), encodedMessage).then(function() {
    return encodedMessage;
  });
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

// move from working set to retry queue
RedisQueue.prototype.fail = function(message) {
  // return this.redisClient.zrem(this.workingSet, message);
  return this.redisClient.scripts.failtask(this.workingSet, message);
};

RedisQueue.prototype.heartbeat = function(message) {
  return this.redisClient.zadd(this.workingSet, Date.now(), message);
};

RedisQueue.prototype._cycle = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var popper = this.poppers.pop();
  
  // prepend with machine/worker id and retry queue
  var prefix = [popper.id || '', this.retryQueue].join('|') + '|';
  
  this.redisClient.scripts.rpopzadd(this.taskQueue, this.workingSet, Date.now(), prefix).then(function(message) {
    if (message) {
      popper.deferred.resolve(encoder.decodeMessage(message));
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
