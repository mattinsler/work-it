exports.Providers = {
  Log: {
    Console: require('./console-log-provider'),
    S3: require('./s3-log-provider')
  },
  Queue: {
    Local: require('./local-queue-provider'),
    Kestrel: require('./kestrel-queue-provider')
  },
  TaskStatus: require('./task-status-provider'),
  TaskStatusStorage: {
    MongoDB: require('./mongodb-task-status-storage-provider')
  },
  TaskStatusEvents: {
    Redis: require('./redis-task-status-events-provider')
  }
};

var getProvider = function(providers, type) {
  var list = Object.keys(providers);
  
  type = type.toLowerCase();
  for (var x = 0; x < list.length; ++x) {
    if (list[x].toLowerCase() === type) { return providers[list[x]]; }
  }
  return null;
};

exports.Providers.Log.get = function(type) { return getProvider(exports.Providers.Log, type); };
exports.Providers.Queue.get = function(type) { return getProvider(exports.Providers.Queue, type); };
exports.Providers.TaskStatusStorage.get = function(type) { return getProvider(exports.Providers.TaskStatusStorage, type); };
exports.Providers.TaskStatusEvents.get = function(type) { return getProvider(exports.Providers.TaskStatusEvents, type); };

exports.Executor = require('./executor');
exports.TaskTracker = require('./task-tracker');
exports.TaskManager = require('./task-manager');
exports.Worker = require('./worker');
