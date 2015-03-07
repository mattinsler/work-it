var q = require('q');

var ConsoleLogger = function(taskId, executionId) {
  this.taskId = taskId;
  this.executionId = executionId;
  
  this.prefix = '[' + this.taskId + ':' + this.executionId + ']> ';
};

ConsoleLogger.prototype.log = function(line) {
  console.log(this.prefix + line);
};

ConsoleLogger.prototype.end = function(line) {
  return q();
};


var ConsoleLogProvider = function() {
  if (!(this instanceof ConsoleLogProvider)) {
    return new ConsoleLogProvider();
  }
};

ConsoleLogProvider.prototype.get = function(taskId, executionId) {
  return new ConsoleLogger(taskId, executionId);
};

module.exports = ConsoleLogProvider;
