var RedisQueue = require('./redis-queue');

module.exports = function(configuration, config) {
  var redisClient = configuration.redisClient;
  var workingSet = configuration.workingSet;
  
  var queueMap = {};
  return {
    get: function(queueName) {
      if (!queueMap[queueName]) {
        queueMap[queueName] = new RedisQueue(redisClient, {
          queue: queueName,
          workingSet: workingSet
          // retry: ''
        });
      }
      
      return queueMap[queueName];
    }
  };
};
