var Worker = require('./worker');
var Providers = require('./providers');
var RedisClient = require('./redis-client');
var TaskManager = require('./task-manager');

var Configuration = function(config) {
  this.config = config;
  this.workingSet = 'working';
  
  var redisClient;
  this.__defineGetter__('redisClient', function() {
    if (!redisClient) {
      redisClient = this.createRedisClient();
    }
    return redisClient;
  });
};

Configuration.prototype.createRedisClient = function() {
  return new RedisClient(this.config.redis, {}, {
    rpopzadd: {
      luafile: require.resolve('../lua/rpopzadd.lua'),
      keys: 2,
      args: 2
    }
  });
};

Configuration.prototype.getQueue = function(queueName, realName) {
  if (!realName) { realName = queueName; }
  var queueConfig = this.config.queues[queueName];
  
  if (!queueConfig) {
    if (queueName === '*') { throw new Error('Could not find a queue configuration for ' + realName + '. You must at least define a queue configuration for *.'); }
    return this.getQueue('*', queueName);
  }
  
  if (queueConfig === 'redis') {
    return Providers.get('queue', 'redis', this).get(realName);
  } else {
    return Providers.get('queue', queueConfig, this).get(realName);
  }
};

Configuration.prototype.worker = function(queueName, handlerPath, opts) {
  if (!opts) { opts = {}; }
  opts.handler = handlerPath;
  return new Worker(this.getQueue(queueName), opts);
};

Configuration.prototype.taskManager = function() {
  return new TaskManager(this);
};

module.exports = Configuration;
