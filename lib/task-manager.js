var q = require('q');
var uuid = require('node-uuid');
var TaskTracker = require('./task-tracker');

var TaskManager = function(opts) {
  if (!(this instanceof TaskManager)) {
    return new TaskManager(opts);
  }
  
  if (!opts) { opts = {}; }
  if (!opts.queueProvider) { throw new Error('You must configure a TaskManager with a queue provider'); }
  
  this.queueProvider = opts.queueProvider;
  this.statusProvider = opts.statusProvider;
};

TaskManager.prototype.queueTask = function(command, data) {
  var task = {
    id: uuid.v4(),
    ts: Date.now(),
    data: data || {}
  };
  
  var taskTracker = new TaskTracker(task.id, this);
  this.queueProvider.get(command).push(task);
  
  return taskTracker;
};

module.exports = TaskManager;
