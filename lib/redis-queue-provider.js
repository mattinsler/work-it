var RedisQueue = require('./redis-queue');
var RedisFairQueue = require('./redis-fair-queue');

module.exports = function(configuration, config) {
  if (!config) { config = {}; }
  var redisClient = configuration.redisClient;
  var workingSet = configuration.workingSet;
  
  var queueMap = {};
  return {
    get: function(queueName) {
      if (!queueMap[queueName]) {
        if (config.fair) {
          queueMap[queueName] = new RedisFairQueue(redisClient, {
            queue: queueName,
            workingSet: workingSet,
            retry: config.retry || null
          });
        } else {
          queueMap[queueName] = new RedisQueue(redisClient, {
            queue: queueName,
            workingSet: workingSet,
            retry: config.retry || null
          });
        }
      }
      
      return queueMap[queueName];
    }
  };
};
