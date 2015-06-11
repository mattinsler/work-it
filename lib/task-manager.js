var q = require('q');
var uuid = require('node-uuid');
var encoder = require('./encoder');
var TaskTracker = require('./task-tracker');

var TaskManager = function(configuration) {
  this.configuration = configuration;
  
  this.workingSet = configuration.workingSet;
  this.redisClient = configuration.redisClient;

  this.__defineGetter__('storage', function() { return configuration.storage(); });
  
  // this.on = this.statusProvider.on.bind(this.statusProvider);
  // this.removeListener = this.statusProvider.removeListener.bind(this.statusProvider);
};

TaskManager.prototype.queueTask = function(queueName, data, opts) {
  var self = this;
  var id = uuid.v4();
  
  return this.configuration.getQueue(queueName).push(id, data, opts).then(function() {
    return self.taskTracker(id);
  });
};

/**
  - should pass opts.popper to set the machine taking the task
  - can optionally override the retry queue by passing opts.retry
  - can set tags by passing opts.tags
*/
TaskManager.prototype.startTaskWork = function(queueName, data, opts) {
  var id = uuid.v4();
  
  // get queue (might not be redis) and pull the retryQueue from it. then get the default queue (will be redis) and adopt on there
  if (!opts) { opts = {}; }
  // override queue name since this isn't a normal path
  opts.queueName = queueName;
  if (!opts.retry) { opts.retry = this.configuration.getQueue(queueName).retryName; }
  
  return this.configuration.getQueue('*').adopt(id, data, opts).then(function() {
    return id;
  });
};

TaskManager.prototype.taskTracker = function(taskId) {
  return new TaskTracker(this, taskId);
};

TaskManager.prototype.getTask = function(id) {
  return this.configuration.getQueue('*').fetchTask(id);
};



TaskManager.prototype.getWorkingTaskList = function(min, max) {
  var redisClient = this.redisClient;
  
  return q().then(function() {
    if (min === undefined && max === undefined) {
      return redisClient.zrange('working', 0, -1, 'WITHSCORES');
    } else if (min === undefined && max !== undefined) {
      return redisClient.zrangebyscore('working', '-inf', max, 'WITHSCORES');
    } else if (min !== undefined && max === undefined) {
      return redisClient.zrangebyscore('working', min, '+inf', 'WITHSCORES');
    } else {
      return redisClient.zrangebyscore('working', min, max, 'WITHSCORES');
    }
  }).then(function(list) {
    var tasks = [];
    for (var x = 0; x < list.length; x += 2) {
      tasks.push({
        id: list[x],
        heartbeat: parseFloat(list[x + 1])
      });
    }
    return tasks;
  });
};

TaskManager.prototype.getNumWorkingTasks = function() {
  return this.redisClient.zcard(this.workingSet);
};

// TaskManager.prototype.getWorkingTasks = function() {
//   return this.getWorkingTaskList().then(function(list) {
//     return list.map(parseTask);
//   });
// };
//
// TaskManager.prototype.getWorkingTasksOlderThan = function(millisOld) {
//   return this.getWorkingTaskList(undefined, Date.now() - millisOld).then(function(list) {
//     return list.map(parseTask);
//   });
// };

TaskManager.prototype.getQueueNames = function() {
  var self = this;
  
  var forPrefix = function(prefix) {
    var queues = [];

    var next = function(lastCursor) {
      var promise;
      if (lastCursor) {
        promise = self.redisClient.scan(lastCursor, 'MATCH', prefix + ':*', 'COUNT', 1000);
      } else {
        promise = self.redisClient.scan('0', 'MATCH', prefix + ':*', 'COUNT', 1000);
      }
      
      return promise.then(function(res) {
        Array.prototype.push.apply(queues, res[1].map(function(queue){ return queue.slice(prefix.length + 1); }));
        if (res[0] === '0') { return queues; }
        return next(res[0]);
      })
    };
    
    return next();
  };

  return q.all([
    forPrefix('q'),
    forPrefix('f')
  ]).spread(function(a, b) {
    return [].concat(a, b);
  });
};

TaskManager.prototype.getQueueStats = function() {
  var self = this;
  var stats = {};
  
  return this.getQueueNames().then(function(queues) {
    return q.all(
      queues.map(function(queue) {
        return self.configuration.getQueue(queue).count().then(function(len) {
          stats[queue] = len;
        });
      })
    );
  }).then(function() {
    return stats;
  });
};

TaskManager.prototype.getDeepQueueStats = function() {
  var self = this;
  var stats = {};
  
  return this.getQueueNames().then(function(queues) {
    return q.all(
      queues.map(function(queue) {
        var queueObject = self.configuration.getQueue(queue);
        return (queueObject.countItems ? queueObject.countItems() : queueObject.count()).then(function(len) {
          stats[queue] = len;
        });
      })
    );
  }).then(function() {
    return stats;
  });
};

module.exports = TaskManager;
