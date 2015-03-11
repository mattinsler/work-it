var fs = require('fs');
var path = require('path');
var workit = require('../');

var Configurer = function(config) {
  if (!(this instanceof Configurer)) {
    return new Configurer(config);
  }
  
  this.config = config;
};

Configurer.prototype.taskManager = function() {
  var config = this.generateConfig();
  
  return workit.TaskManager(config);
};

Configurer.prototype.worker = function(commandName, workerFile, opts) {
  if (!opts) { opts = {}; }
  if (!fs.existsSync(workerFile)) { throw new Error('Could not find worker file at ' + workerFile); }
  
  var config = this.generateConfig();
  
  config.handler = workit.Executor.create(workerFile, {
    logProvider: this.getProvider(workit.Providers.Log, this.config.logProvider)
  });
  
  config.command = commandName;
  config.concurrency = opts.concurrency || 1;
  
  return workit.Worker(config);
};

Configurer.prototype.generateConfig = function() {
  var opts = {};
  
  opts.queueProvider = this.getProvider(workit.Providers.Queue, this.config.queueProvider);
  
  opts.statusProvider = workit.Providers.TaskStatus({
    storageProvider: this.getProvider(workit.Providers.TaskStatusStorage, this.config.statusProvider.storageProvider),
    eventsProvider: this.getProvider(workit.Providers.TaskStatusEvents, this.config.statusProvider.eventsProvider)
  });
  
  return opts;
};

Configurer.prototype.getProvider = function(providers, configValue) {
  var type = Object.keys(configValue)[0];
  var value = configValue[type];
  var providerType = providers.get(type);
  if (!providerType) {
    throw new Error('Could not find provider ' + type);
  }
  return providerType(value);
};

Configurer.fromFile = function(filename) {
  if (!fs.existsSync(filename)) { throw new Error('Could not file configuration file at ' + filename); }
  if (path.extname(filename) !== '.json') { throw new Error(''); }
  
  var config;
  try {
    config = require(filename);
  } catch (err) {
    throw new Error('Could not parse configuration file at ' + filename + ': ' + err.message);
  }
  
  return new Configurer(config);
};

module.exports = Configurer;
