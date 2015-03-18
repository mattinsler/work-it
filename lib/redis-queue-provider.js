var q = require('q');
var redis = require('redis');
var redisBuilder = require('redis-builder')(redis);
var encoder = require('./encoder');

var RedisQueue = function(provider, name, opts) {
  this.name = name;
  this.opts = opts || {};
  
  this.taskQueue = 'q:' + this.name;
  this.workQueue = 'w:' + this.name;
  
  this.poppers = [];
  this.idMessageMap = {};
  
  var popClient;
  this.__defineGetter__('popClient', function() {
    if (!popClient) {
      popClient = provider.createConnection();
    }
    return popClient;
  });
  var pushClient;
  this.__defineGetter__('pushClient', function() {
    if (!pushClient) {
      pushClient = provider.createConnection();
    }
    return pushClient;
  });
};

RedisQueue.prototype.push = function(message) {
  if (!message) { return q(); }
  if (!message.id) { return q.reject(new Error('Messages pushed to a RedisQueue must include an id')); }
  return q.ninvoke(this.pushClient, 'lpush', this.taskQueue, encoder.encode(message));
};

RedisQueue.prototype.pop = function() {
  var d = q.defer();
  
  this.poppers.unshift(d);
  setImmediate(this._churnPop.bind(this));
  
  return d.promise;
};

RedisQueue.prototype.complete = function(messageID) {
  var message = this.idMessageMap[messageID];
  delete this.idMessageMap[messageID];
  
  if (!message) { return q(); }
  return q.ninvoke(this.pushClient, 'lrem', this.workQueue, -1, message);
};

RedisQueue.prototype.abort = function(messageID) {
  var message = this.idMessageMap[messageID];
  delete this.idMessageMap[messageID];
  
  if (!message) { return q(); }
  
  return q.ninvoke(
    this.pushClient.multi()
      .lpush(this.taskQueue, message)
      .lrem(this.workQueue, -1, message)
    , 'exec'
  );
  
  return q.ninvoke(this.pushClient, 'lrem', this.workQueue, -1, message);
};

RedisQueue.prototype._churnPop = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var client = this.popClient;
  q.ninvoke(client, 'brpoplpush', this.taskQueue, this.workQueue, 5).then(function(message) {
    if (message) {
      var popper = self.poppers.pop();
      
      if (popper) {
        var task = encoder.decode(message);
        
        self.idMessageMap[task.id] = message;
        
        popper.resolve({
          id: task.id,
          task: task
        });
      } else {
        // oops, no one to give this task to... try to back up
        return q.ninvoke(
          client.multi()
            .rpush(self.taskQueue, message)
            .lrem(self.workQueue, -1, message)
          , 'exec'
        );
      }
    }
  }).catch(function(err) {
    console.log(err.stack);
  }).finally(function() {
    setImmediate(self._churnPop.bind(self));
  });
};



var RedisQueueProvider = function(redisUrl) {
  if (!(this instanceof RedisQueueProvider)) {
    return new RedisQueueProvider(redisUrl);
  }
  
  this.redisUrl = redisUrl;
  this.queueMap = {};
};

RedisQueueProvider.prototype.get = function(name, opts) {
  if (!this.queueMap[name]) {
    this.queueMap[name] = new RedisQueue(this, name, opts);
  }
  
  return this.queueMap[name];
};

RedisQueueProvider.prototype.createConnection = function() {
  return redisBuilder(this.redisUrl);
};

module.exports = RedisQueueProvider;
