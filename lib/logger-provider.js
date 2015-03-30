var q = require('q');

var Logger = function(loggers) {
  this.loggers = loggers;
};

Logger.prototype.log = function(line) {
  var args = Array.prototype.slice.call(arguments);
  
  this.loggers.forEach(function(logger) {
    logger.log.apply(logger, args);
  });
};

Logger.prototype.end = function(line) {
  var args = Array.prototype.slice.call(arguments);
  
  return q.all(
    this.loggers.map(function(logger) {
      return logger.end.apply(logger, args);
    })
  );
};



var LoggerProvider = function(providers) {
  if (!(this instanceof LoggerProvider)) {
    return new LoggerProvider(providers);
  }
  
  this.providers = providers;
};

LoggerProvider.prototype.get = function(taskId, executionId) {
  var args = Array.prototype.slice.call(arguments);
  
  return new Logger(
    this.providers.map(function(provider) {
      return provider.get.apply(provider, args);
    })
  );
};

module.exports = LoggerProvider;
