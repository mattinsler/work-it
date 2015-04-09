var Reaper = function(configuration) {
  this.workingSet = configuration.workingSet;
  this.redisClient = configuration.redisClient;
};

Reaper.prototype.reap = function(taskTimeout) {
  return this.redisClient.scripts.reaptasks(this.workingSet, Date.now() - taskTimeout).then(function(tasks) {
    return {
      reaped: tasks[0],
      skipped: tasks[1]
    }
  });
};

module.exports = Reaper;
