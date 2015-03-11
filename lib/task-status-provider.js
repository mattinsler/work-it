var q = require('q');

var TaskStatus = function(provider, taskId, executionId) {
  this.taskId = taskId;
  this.executionId = executionId;
  
  var cachedStorage;
  this.__defineGetter__('storage', function() {
    if (!cachedStorage) {
      cachedStorage = provider.storageProvider.get(taskId, executionId);
    }
    return cachedStorage;
  });
  
  var cachedEvents;
  this.__defineGetter__('events', function() {
    if (!cachedEvents) {
      cachedEvents = provider.eventsProvider.get(taskId, executionId);
    }
    return cachedEvents;
  });
};

TaskStatus.prototype.on = function(event, callback) {
  this.events.on(event, callback);
};

TaskStatus.prototype.removeListener = function(event, callback) {
  this.events.removeListener(event, callback);
};

TaskStatus.prototype.start = function(statusUpdate) {
  return q.all([
    this.storage.start(statusUpdate),
    this.events.start(statusUpdate)
  ]);
};

TaskStatus.prototype.finish = function(statusUpdate) {
  return q.all([
    this.storage.finish(statusUpdate),
    this.events.finish(statusUpdate)
  ]);
};



var TaskStatusProvider = function(opts) {
  if (!(this instanceof TaskStatusProvider)) {
    return new TaskStatusProvider(opts);
  }
  
  this.storageProvider = opts.storageProvider;
  this.eventsProvider = opts.eventsProvider;
  
  this.on = this.eventsProvider.on.bind(this.eventsProvider);
  this.removeListener = this.eventsProvider.removeListener.bind(this.eventsProvider);
};

TaskStatusProvider.prototype.get = function(taskId, executionId) {
  return new TaskStatus(this, taskId, executionId);
};

TaskStatusProvider.prototype.getTaskList = function(command) {
  return this.storageProvider.getTaskList(command);
};

TaskStatusProvider.prototype.getTask = function(id) {
  return this.storageProvider.getTask(id);
};

module.exports = TaskStatusProvider;
