var RedisQueue = require('./redis-queue');
var RedisFairQueue = require('./redis-fair-queue');
var RedisPayloadQueue = require('./redis-payload-queue');

module.exports = function(configuration, config) {
  if (!config) { config = {}; }
  var redisClient = configuration.redisClient;
  var workingSet = configuration.workingSet;

  var payloadStorageProvider;
  if (configuration.config.payload) {
    var Providers = require('./providers');
    payloadStorageProvider = Providers.get('payloadstorage', configuration.config.payload, configuration);
  }

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
          if (payloadStorageProvider) {
            queueMap[queueName] = new RedisPayloadQueue(redisClient, {
              queue: queueName,
              workingSet: workingSet,
              retry: config.retry || null,
              payloadStorage: payloadStorageProvider.get()
            });
          } else {
            queueMap[queueName] = new RedisQueue(redisClient, {
              queue: queueName,
              workingSet: workingSet,
              retry: config.retry || null
            });
          }
        }
      }
      
      return queueMap[queueName];
    }
  };
};
