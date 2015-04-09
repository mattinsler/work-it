var Worker = require('./worker');
var Reaper = require('./reaper');
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
    poptask: {
      luafile: require.resolve('../lua/poptask.lua'),
      keys: 2,
      args: 1
    },
    completetask: {
      luafile: require.resolve('../lua/completetask.lua'),
      keys: 1,
      args: 1
    },
    failtask: {
      luafile: require.resolve('../lua/failtask.lua'),
      keys: 1,
      args: 1
    },
    reaptasks: {
      luafile: require.resolve('../lua/reaptasks.lua'),
      keys: 1,
      args: 1
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
  opts.machineId = opts['machine-id'];
  delete opts['machine-id'];
  
  var self = this;
  var loggerProviders = (this.config.loggers || []).map(function(logConfig) {
    return Providers.get('logger', logConfig, self);
  });
  if (loggerProviders.length === 0) {
    loggerProviders.push(Providers.get('logger', 'console', this));
  }
  opts.loggerProvider = require('./logger-provider')(loggerProviders);
  
  opts.storageProvider = Providers.get('storage', this.config.storage, this);
  
  return new Worker(this.getQueue(queueName), opts);
};

Configuration.prototype.taskManager = function() {
  return new TaskManager(this);
};

Configuration.prototype.reaper = function() {
  return new Reaper(this);
};

module.exports = Configuration;
