var q = require('q');
var Backoff = require('./backoff');
var encoder = require('./encoder');

var RedisQueue = function(redisProvider, opts) {
  this.name = opts.queue;
  this.workingSet = opts.workingSet;
  
  this.taskQueue = 'q:' + this.name;
  this.retryQueue = 'q:' + (opts.retry || this.name);
  
  this.poppers = [];
  
  this.backoff = new Backoff(this._cycle, this);
  
  var pushClient;
  this.__defineGetter__('pushClient', function() {
    if (!pushClient) {
      pushClient = redisProvider();
    }
    return pushClient;
  });
  
  var popClient;
  this.__defineGetter__('popClient', function() {
    if (!popClient) {
      popClient = redisProvider();
    }
    return popClient;
  });
};

RedisQueue.prototype.push = function(message) {
  if (!message) { return q(); }
  if (!message.id) { return q.reject(new Error('Messages pushed to a RedisQueue must include an id')); }
  return this.pushClient.lpush(this.taskQueue, encoder.encode(message));
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
  return this.pushClient.zrem(this.workingSet, message);
};

RedisQueue.prototype.heartbeat = function(message) {
  return this.pushClient.zadd(this.workingSet, Date.now(), message);
};

RedisQueue.prototype._cycle = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var popper = this.poppers.pop();
  
  // need to append or prepend retry queue
  // should also append/prepend machine-id/worker-id
  // pass machine/worker ID to rpopzadd so that it exists in the working set
  
  // prepend with retry queue
  var prefix = (popper.id || '') + '|' + this.retryQueue + '|';
  
  this.popClient.scripts.rpopzadd(this.taskQueue, this.workingSet, Date.now(), prefix).then(function(message) {
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
