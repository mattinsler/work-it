var Reaper = function(configuration) {
  this.redisClient = configuration.redisClient;
};

Reaper.prototype.reap = function(taskTimeout) {
  return this.redisClient.scripts.retrytasks('working', Date.now() - taskTimeout);
};

module.exports = Reaper;
