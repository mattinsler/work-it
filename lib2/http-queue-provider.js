var HttpQueue = require('./http-queue');

module.exports = function(configuration, config) {
  if (typeof(config) === 'string') { config = {url: config}; }
  if (!config.method) { config.method = 'get'; }
  config.method = config.method.toLowerCase();
  
  if (['get', 'post', 'put'].indexOf(config.method) === -1) { throw new Error('HTTP Queue Provider does not work with ' + config.method + ' requests'); }
  
  var redisClient = configuration.redisClient;
  var workingSet = configuration.workingSet;
  
  var queueMap = {};
  return {
    get: function(queueName) {
      if (!queueMap[queueName]) {
        queueMap[queueName] = new HttpQueue(redisClient, config, {
          queue: queueName,
          workingSet: workingSet
          // retry: ''
        });
      }
      
      return queueMap[queueName];
    }
  };
};
