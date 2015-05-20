var q = require('q');
var Backoff = require('./backoff');
var encoder = require('./encoder');
var debug = require('debug')('work-it:redis-queue');

var RedisFairQueue = function(redisClient, opts) {
  if (!opts.retry || opts.retry === opts.queue) {
    throw new Error('Fair queues must specify a retry queue that is not the same queue');
  }

  this.name = opts.queue;
  this.retryName = (opts.retry || this.name);
  this.workingSet = opts.workingSet;
  this.redisClient = redisClient;
  
  this.taskQueue = 'f:' + this.name;
  this.retryQueue = 'q:' + this.retryName;
  
  this.poppers = [];
  
  this.backoff = new Backoff(this._cycle, this);
};

RedisFairQueue.STATE = {
  q: 'QUEUED',
  QUEUED: 'q',
  w: 'WORKING',
  WORKING: 'w'
};

// add fair key?
RedisFairQueue.prototype._formatTask = function(task, opts) {
  if (!opts) { opts = {}; }
  
  return {
    fc: 0,                              // failed
    rc: 0,                              // reaped
    s: Date.now(),                      // timestamp
    d: encoder.encode(task),            // task data
    t: encoder.encode(opts.tags || []), // tags
    q: opts.queueName || this.name,
    r: opts.retry || this.retryName,
    a: RedisFairQueue.STATE.QUEUED          // current state
  };
};

// add fair key?
RedisFairQueue.prototype._parseTask = function(data) {
  return {
    id: data.id,
    queue: data.q,
    retryQueue: data.r,
    queuedAt: new Date(parseFloat(data.s)),
    data: encoder.decode(data.d),
    tags: encoder.decode(data.t),
    failedCount: parseInt(data.fc),
    reapedCount: parseInt(data.rc),
    currentState: RedisFairQueue.STATE[data.a]
  };
};

RedisFairQueue.prototype.clear = function() {
  // this is pretty expensive, but is probably good to do this right
  return this.redisClient.scripts.fairclearqueue(this.taskQueue);
};

RedisFairQueue.prototype.clearFairKey = function(fairKey) {
  return this.redisClient.scripts.fairclearkey(this.taskQueue, fairKey);
};

RedisFairQueue.prototype.count = function() {
  // could be really expensive...
  return this.redisClient.scripts.faircountqueue(this.taskQueue);
};

RedisFairQueue.prototype.fetchTask = function(id) {
  return this.redisClient.hgetall('t:' + id).then(this._parseTask).then(function(data) {
    data.id = id;
    return data;
  });
};

RedisFairQueue.prototype.push = function(id, task, opts) {
  if (!id || !task || !opts || !opts.fairKey) { return q.reject(new Error('RedisFairQueue::push must be passed (id, task, {fairKey: ...})')); }
  
  var data = this._formatTask(task, opts);
  
  var self = this;
  return this.redisClient._connect().then(function(client) {
    var multi = client.multi().hmset('t:' + id, data);
    self.redisClient.multiScripts.fairpushtask(multi, self.name, id, opts.fairKey);
    return q.ninvoke(multi, 'exec');
  });
};

RedisFairQueue.prototype.adopt = function(id, task, opts) {
  return q.reject(new Error('RedisFairQueue::adopt is not supported'));
};

RedisFairQueue.prototype.pop = function(id) {
  var d = q.defer();
  
  this.poppers.unshift({
    id: id,
    deferred: d
  });
  this.backoff.immediate();
  
  return d.promise;
};

// remove task and from working set
RedisFairQueue.prototype.complete = function(task) {
  if (!task) { return q.reject(new Error('RedisFairQueue::complete must be passed a task object or task ID')); }
  
  var id = task.id ? task.id : task;
  return this.redisClient.scripts.completetask(this.workingSet, id);
};

// increase failed count in task and add to retry queue
RedisFairQueue.prototype.fail = function(task) {
  if (!task) { return q.reject(new Error('RedisFairQueue::fail must be passed a task object or task ID')); }
  
  var id = task.id ? task.id : task;
  return this.redisClient.scripts.failtask(this.workingSet, task);
};

RedisFairQueue.prototype.heartbeat = function(task) {
  if (!task) { return q.reject(new Error('RedisFairQueue::heartbeat must be passed a task object or task ID')); }
  
  var id = task.id ? task.id : task;
  return this.redisClient.zadd(this.workingSet, Date.now(), id);
};

RedisFairQueue.prototype._cycle = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var popper = this.poppers.pop();
  
  this.redisClient.scripts.fairpoptask(this.name, this.workingSet, Date.now()).then(function(task) {
    if (task) {
      var taskData = {};
      for (var x = 0; x < task.length; x += 2) {
        taskData[task[x]] = task[x + 1];
      }
      
      try {
        taskData = self._parseTask(taskData);
      } catch (err) {
        debug('There was an error in popping or parsing this task: ' + JSON.stringify(taskData));
        return self.backoff.next();
      }
      
      popper.deferred.resolve(taskData);
      self.backoff.reset();
    } else {
      self.poppers.push(popper);
      self.backoff.next();
    }
  }).catch(function(err) {
    console.log(err.stack);
    self.backoff.next();
  });
};

module.exports = RedisFairQueue;
