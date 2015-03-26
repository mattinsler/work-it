var redis = require('redis');
var builder = require('redis-builder')(redis);

var RedisMonitoring = function(redisUrl, opts) {
  console.log('RedisHeartbeat', arguments);
  
  if (!(this instanceof RedisHeartbeat)) {
    return new RedisHeartbeat(redisUrl, opts);
  }
  
  this.key = (opts && opts.key) ? opts.key : 'worker-heartbeat';
  this.interval = (opts && opts.interval) ? opts.interval : 1000;
  
  var client;
  this.__defineGetter__('client', function() {
    if (!client) {
      client = builder(redisUrl);
    }
    return client;
  });
};

// RedisHeartbeat.prototype.start = function() {
//   if (this.intervalId) { return; }
//   this.intervalId = setInterval(this.beat.bind(this), this.interval);
// };
//
// RedisHeartbeat.prototype.stop = function() {
//   if (!this.intervalId) { return; }
//   clearInterval(this.intervalId);
//   delete this.intervalId;
// };

RedisHeartbeat.prototype.beat = function(id) {
  this.client.zadd(this.key, Date.now(), id);
};




var RedisMonitoringProvider = function(redisUrl) {
  if (!(this instanceof RedisMonitoringProvider)) {
    return new RedisMonitoringProvider(redisUrl);
  }
  
  this.redisUrl = redisUrl;
};

RedisMonitoringProvider.prototype.get = function(name, opts) {
  return new RedisMonitoring(name, opts);
};

RedisMonitoringProvider.prototype.createConnection = function() {
  return redisBuilder(this.redisUrl);
};

module.exports = RedisMonitoringProvider;
