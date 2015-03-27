var q = require('q');

var ConsoleLogger = function(task, execution) {
  this.task = task;
  this.execution = execution;
  
  this.prefix = '[' + this.task + ':' + this.execution + ']> ';
};

ConsoleLogger.prototype.log = function(line) {
  console.log(this.prefix + line);
};

ConsoleLogger.prototype.end = function(line) {
  return q();
};

module.exports = function(configuration) {
  return {
    get: function(task, execution) {
      return new ConsoleLogger(task, execution);
    }
  };
};
