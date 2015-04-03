var q = require('q');
var uuid = require('node-uuid');
var TaskTracker = require('./task-tracker');

var TaskManager = function(configuration) {
  this.configuration = configuration;
  
  this.workingSet = configuration.workingSet;
  this.redisClient = configuration.redisClient;
  
  // this.on = this.statusProvider.on.bind(this.statusProvider);
  // this.removeListener = this.statusProvider.removeListener.bind(this.statusProvider);
};

TaskManager.prototype.createTaskObject = function(data, opts) {
  if (!opts) { opts = {}; }
  
  return {
    id: uuid.v4(),
    ts: Date.now(),
    data: data || {},
    tags: opts.tags || []
  };
};

TaskManager.prototype.queueTask = function(queueName, data, opts) {
  var task = this.createTaskObject(data, opts);
  
  var self = this;
  return this.configuration.getQueue(queueName).push(task).then(function() {
    return self.taskTracker(task.id);
  });
};

/**
  - should pass opts.popper to set the machine taking the task
  - can optionally override the retry queue by passing opts.retry
  - can set tags by passing opts.tags
*/
TaskManager.prototype.startTaskWork = function(queueName, data, opts) {
  if (!opts) { opts = {}; }
  
  var task = this.createTaskObject(data, opts);
  return this.configuration.getQueue(queueName).adopt(task, {popper: opts.popper, retry: opts.retry});
};

TaskManager.prototype.taskTracker = function(taskId) {
  return new TaskTracker(this, taskId);
};

var parseTask = function(item) {
  var task = encoder.decodeMessage(item[0]);
  task.heartbeat = new Date(parseInt(item[1]));
  
  return task;
};

TaskManager.prototype._workingTaskList = function(min, max) {
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
      tasks.push([list[x], list[x + 1]]);
    }
    return tasks;
  });
};

TaskManager.prototype.workingTasks = function() {
  return this._workingTaskList().then(function(list) {
    return list.map(parseTask);
  });
};

TaskManager.prototype.workingTasksOlderThan = function(millisOld) {
  return this._workingTaskList(undefined, Date.now() - millisOld).then(function(list) {
    return list.map(parseTask);
  });
};

TaskManager.prototype.getQueueNames = function() {
  var self = this;
  var queues = [];
  
  var next = function(lastCursor) {
    var promise;
    if (lastCursor) {
      promise = self.redisClient.scan(lastCursor);
    } else {
      promise = self.redisClient.scan('0', 'MATCH', 'q:*');
    }
    
    return promise.then(function(res) {
      Array.prototype.push.apply(queues, res[1].map(function(queue){ return queue.replace(/^q:/, ''); }));
      if (res[0] === '0') { return queues; }
      return next(res[0]);
    })
  };
  
  return next();
};

TaskManager.prototype.getQueueStats = function() {
  var self = this;
  var stats = {};
  
  return this.getQueueNames().then(function(queues) {
    return q.all(
      queues.map(function(queue) {
        return self.redisClient.llen('q:' + queue).then(function(len) {
          stats[queue] = len;
        })
      })
    )
  }).then(function() {
    return stats;
  });
};

module.exports = TaskManager;
