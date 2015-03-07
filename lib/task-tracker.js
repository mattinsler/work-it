var q = require('q');

var TaskTracker = function(id, taskManager) {
  if (!(this instanceof TaskTracker)) {
    return new TaskTracker(id, taskManager);
  }
  
  this.id = id;
  this.taskManager = taskManager;
  this.status = this.taskManager.statusProvider.get(this.id);
  
  this.on = this.status.on.bind(this.status);
  this.removeListener = this.status.removeListener.bind(this.status);
};

module.exports = TaskTracker;
