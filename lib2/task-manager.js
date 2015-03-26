var q = require('q');
var uuid = require('node-uuid');
var encoder = require('./encoder');
var TaskTracker = require('./task-tracker');

var TaskManager = function(configuration) {
  this.configuration = configuration;
  
  // this.on = this.statusProvider.on.bind(this.statusProvider);
  // this.removeListener = this.statusProvider.removeListener.bind(this.statusProvider);
  
  var redisClient;
  this.__defineGetter__('redisClient', function() {
    if (!redisClient) {
      redisClient = this.configuration.createRedisClient();
    }
    return redisClient;
  });
};

TaskManager.prototype.queueTask = function(queueName, data) {
  var task = {
    id: uuid.v4(),
    ts: Date.now(),
    data: data || {}
  };
  
  var self = this;
  return this.configuration.getQueue(queueName).push(task).then(function() {
    return self.taskTracker(task.id);
  });
};

TaskManager.prototype.taskTracker = function(taskId) {
  return new TaskTracker(this, taskId);
};

var parseTask = function(item) {
  var task = {
    id: item[0],
    heartbeat: new Date(parseInt(item[1]))
  };
  var match = /^([^|]*)\|([^|]+)\|(.*)$/.exec(item[0]);
  task.worker = match[1];
  task.retryQueue = match[2];
  task.task = encoder.decode(match[3]);
  
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

// TaskManager.prototype.retryTasksOlderThan = function(millisOld) {
//   return this._workingTaskList(undefined, Date.now() - millisOld).then(function(list) {
//
//   });
// };

module.exports = TaskManager;
