var q = require('q');
var fs = require('fs');
var redis = require('redis');
var redisBuilder = require('redis-builder')(redis);
var Backoff = require('./backoff');
var encoder = require('./encoder');

var RPOPZADD = fs.readFileSync(require.resolve('../lua/rpopzadd.lua')).toString();

var RedisQueue = function(provider, name, opts) {
  this.provider = provider;
  this.name = name;
  this.opts = opts || {};
  
  this.taskQueue = 'q:' + this.name;
  this.workQueue = 'w:' + this.name;
  
  this.poppers = [];
  this.idMessageMap = {};
  
  this.backoff = new Backoff(this._churnPop, this);
  
  this.scripts = {};
  
  var pushClient;
  this.__defineGetter__('pushClient', function() {
    if (!pushClient) {
      pushClient = provider.createConnection();
    }
    return pushClient;
  });
};

RedisQueue.prototype.getPopClient = function() {
  if (!this.popClientDeferred) {
    this.popClientDeferred = q.defer();
    
    var self = this;
    var client = this.provider.createConnection();
    client.script('load', RPOPZADD, function(err, sha) {
      if (err) { return console.log('Could not load RPOPZADD', err.stack); }
      self.scripts.rpopzadd = function(popfrom, zaddto, score, callback) {
        console.log('rpopzadd', popfrom, zaddto, score);
        client.evalsha(sha, 2, popfrom, zaddto, score, callback);
      };
      
      self.popClientDeferred.resolve(client);
    });
  }
  return this.popClientDeferred.promise;
};

RedisQueue.prototype.push = function(message) {
  if (!message) { return q(); }
  if (!message.id) { return q.reject(new Error('Messages pushed to a RedisQueue must include an id')); }
  return q.ninvoke(this.pushClient, 'lpush', this.taskQueue, encoder.encode(message));
};

RedisQueue.prototype.pop = function() {
  var d = q.defer();
  
  this.poppers.unshift(d);
  this.backoff.immediate();
  
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
  this.getPopClient().then(function(client) {
    return q.ninvoke(self.scripts, 'rpopzadd', self.taskQueue, self.workQueue, Date.now()).then(function(message) {
      if (message) {
        var popper = self.poppers.pop();
      
        if (popper) {
          var task = encoder.decode(message);
        
          self.idMessageMap[task.id] = message;
        
          popper.resolve({
            id: task.id,
            task: task
          });
          
          self.backoff.reset();
        }
      }
      
      self.backoff.next();
    });
  }).catch(function(err) {
    console.log(err.stack);
    self.backoff.next();
  });
};



var RedisQueueProvider = function(redisUrl, opts) {
  if (!(this instanceof RedisQueueProvider)) {
    return new RedisQueueProvider(redisUrl, opts);
  }
  
  this.redisUrl = redisUrl;
  this.queueMap = {};
  
  if (opts && opts.monitoring) {
    this.monitoring = opts.monitoring;
  }
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
