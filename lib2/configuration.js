var Worker = require('./worker');
var RedisQueue = require('./redis-queue');
var RedisClient = require('./redis-client');
var TaskManager = require('./task-manager');

var Configuration = function(config) {
  this.config = config;
  
  this.workingSet = 'working';
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
  if (this.config.queues[queueName]) {
    if (this.config.queues[queueName] === 'redis') {
      var redisProvider = this.createRedisClient.bind(this);
      return new RedisQueue(redisProvider, {
        queue: realName || queueName,
        workingSet: 'working'
        // retry: ''
      });
    }
  } else {
    return this.getQueue('*', queueName);
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
