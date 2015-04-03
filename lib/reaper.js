var Reaper = function(configuration) {
  this.redisClient = configuration.redisClient;
};

Reaper.prototype.reap = function(taskTimeout) {
  return this.redisClient.scripts.reaptasks('working', Date.now() - taskTimeout);
};

module.exports = Reaper;
