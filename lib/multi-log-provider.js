var q = require('q');

var MultiLogger = function(loggers) {
  this.loggers = loggers;
};

MultiLogger.prototype.log = function(line) {
  this.loggers.forEach(function(logger) {
    logger.log(line);
  });
};

MultiLogger.prototype.end = function(line) {
  return q.all(
    this.loggers.map(function(logger) {
      return logger.end(line);
    })
  );
};



var MultiLogProvider = function(providerList) {
  if (!(this instanceof MultiLogProvider)) {
    return new MultiLogProvider(providerList);
  }
  
  this.providerList = providerList;
};

MultiLogProvider.prototype.get = function(taskId, executionId) {
  return new MultiLogger(
    this.providerList.map(function(provider) {
      return provider.get(taskId, executionId);
    })
  );
};

module.exports = MultiLogProvider;
