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

exports.Executor = require('./executor');
exports.TaskTracker = require('./task-tracker');
exports.TaskManager = require('./task-manager');
exports.Worker = require('./worker');
