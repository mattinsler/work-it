var q = require('q');
var Backoff = require('./backoff');
var encoder = require('./encoder');
var debug = require('debug')('work-it:redis-queue');

var RedisQueue = function(redisClient, opts) {
  this.name = opts.queue;
  this.retryName = (opts.retry || this.name);
  this.workingSet = opts.workingSet;
  this.redisClient = redisClient;
  
  this.taskQueue = 'q:' + this.name;
  this.retryQueue = 'q:' + this.retryName;
  
  this.poppers = [];
  
  this.backoff = new Backoff(this._cycle, this);
};

RedisQueue.STATE = {
  q: 'QUEUED',
  QUEUED: 'q',
  w: 'WORKING',
  WORKING: 'w'
};

RedisQueue.prototype._formatTask = function(task, opts) {
  if (!opts) { opts = {}; }
  
  return {
    fc: 0,                              // failed
    rc: 0,                              // reaped
    s: Date.now(),                      // timestamp
    d: encoder.encode(task),            // task data
    t: encoder.encode(opts.tags || []), // tags
    q: opts.queueName || this.name,
    r: opts.retry || this.retryName,
    a: RedisQueue.STATE.QUEUED          // current state
  };
};

RedisQueue.prototype._parseTask = function(data) {
  return {
    id: data.id,
    queue: data.q,
    retryQueue: data.r,
    queuedAt: new Date(parseFloat(data.s)),
    data: encoder.decode(data.d),
    tags: encoder.decode(data.t),
    failedCount: parseInt(data.fc),
    reapedCount: parseInt(data.rc),
    currentState: RedisQueue.STATE[data.a]
  };
};

RedisQueue.prototype.clear = function() {
  return this.redisClient.scripts.clearqueue(this.taskQueue);
};

RedisQueue.prototype.count = function() {
  return this.redisClient.llen(this.taskQueue);
};

RedisQueue.prototype.fetchTask = function(id) {
  return this.redisClient.hgetall('t:' + id).then(this._parseTask).then(function(data) {
    data.id = id;
    return data;
  });
};

RedisQueue.prototype.push = function(id, task, opts) {
  if (!id || !task) { return q.fail(new Error('RedisQueue::push must be passed (id, task)')); }
  
  var data = this._formatTask(task, opts);
  
  var self = this;
  return this.redisClient._connect().then(function(client) {
    return q.ninvoke(
      client.multi()
        .hmset('t:' + id, data)
        .lpush(self.taskQueue, id)
    , 'exec');
  });
};

RedisQueue.prototype.adopt = function(id, task, opts) {
  if (!id || !task) { return q.fail(new Error('RedisQueue::adopt must be passed (id, task)')); }
  
  var data = this._formatTask(task, opts);
  
  // add task hash to redis and then add to working set (run pop)
  // do a multi that creates the task and then calls poptask on it...
  
  var self = this;
  return this.redisClient._connect().then(function(client) {
    var multi = client.multi().hmset('t:' + id, data);
    self.redisClient.multiScripts.adopttask(multi, self.workingSet, id, Date.now());
    return q.ninvoke(multi, 'exec');
  });
};

RedisQueue.prototype.pop = function(id) {
  var d = q.defer();
  
  this.poppers.unshift({
    id: id,
    deferred: d
  });
  this.backoff.immediate();
  
  return d.promise;
};

// rmeove task and from working set
RedisQueue.prototype.complete = function(task) {
  if (!task) { return q.reject(new Error('RedisQueue::complete must be passed a task object or task ID')); }
  
  var id = task.id ? task.id : task;
  return this.redisClient.scripts.completetask(this.workingSet, id);
};

// increase failed count in task and add to retry queue
RedisQueue.prototype.fail = function(task) {
  if (!task) { return q.reject(new Error('RedisQueue::fail must be passed a task object or task ID')); }
  
  var id = task.id ? task.id : task;
  return this.redisClient.scripts.failtask(this.workingSet, task);
};

RedisQueue.prototype.heartbeat = function(task) {
  if (!task) { return q.reject(new Error('RedisQueue::heartbeat must be passed a task object or task ID')); }
  
  var id = task.id ? task.id : task;
  return this.redisClient.zadd(this.workingSet, Date.now(), id);
};

RedisQueue.prototype._cycle = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var popper = this.poppers.pop();
  
  // prepend with machine/worker id and retry queue
  // var prefix = [popper.id || '', this.retryQueue].join('|') + '|';
  
  this.redisClient.scripts.poptask(this.taskQueue, this.workingSet, Date.now()).then(function(task) {
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

module.exports = RedisQueue;
