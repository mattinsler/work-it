var q = require('q');
var moment = require('moment');
var ChildProcess = require('./child-process');

var debug = require('debug')('work-it:worker');

var DEFAULT_CONCURRENCY = 1;
var DEFAULT_EXECUTION_LIMIT = 25;

var duration = function(millis) {
  var d = moment.duration(millis);
  return d.minutes() + 'm ' + d.seconds() + 's ' + d.milliseconds() + 'ms';
};

var Worker = function(queue, opts) {
  this.queue = queue;
  
  if (!opts) { opts = {}; }
  if (!opts.handler) { throw new Error('You must configure a worker with a handler'); }
  
  this.executionLimit = opts.executionLimit || DEFAULT_EXECUTION_LIMIT;
  this.concurrency = opts.concurrency || DEFAULT_CONCURRENCY;
  this.handlerPath = opts.handler;
  this.loggerProvider = opts.loggerProvider;
  this.storageProvider = opts.storageProvider;
  
  // if (!opts.command) { throw new Error('You must configure a Worker with a command'); }
  // if (!opts.queueProvider) { throw new Error('You must configure a Worker with a queue provider'); }
  
  this.running = false;
  this.children = {
    active: {},
    inactive: []
  };
  
  this.taskData = {};
};

Worker.prototype.start = function() {
  if (this.running === true) { return q(); }
  this.running = true;
  
  debug();
  debug('Worker configuration:');
  debug('  - Command        :', this.queue.name);
  debug('  - Handler        :', this.handlerPath);
  debug('  - Concurrency    :', this.concurrency);
  debug('  - Execution Limit:', this.executionLimit);
  debug();
  debug('Starting ' + this.concurrency + ' workers...');
  
  this.onHeartbeat = function(worker) {
    if (!worker.taskId) { return; }
    this.queue.heartbeat(worker.envelopeId);
  }.bind(this);
  
  
  for (var x = 0; x < this.concurrency; ++x) {
    this.children.inactive.push(this.createChild());
  }
  
  var self = this;
  return q.all(
    this.children.inactive.map(function(w) {
      return w.start();
    })
  ).then(function() {
    debug('All ' + self.children.inactive.length + ' workers started');
    debug();
    
    setImmediate(self._cycle.bind(self));
  });
};

// Worker.prototype.monitorResources = function() {
//   var self = this;
//
//   q.all(
//     Object.keys(self.children).map(function(key) {
//       var worker = self.children[key];
//       return self.taskStats(worker).then(function(stats) {
//         stats.ts = Date.now() - worker.startedAt;
//         worker.stats.push(stats);
//         console.log([
//           'ts : ' + stats.ts,
//           'cpu: ' + stats.cpu.user + '/' + stats.cpu.system,
//           'mem: ' + stats.mem.resident + '/' + stats.mem.virtual
//         ].join('\n'));
//       });
//     })
//   );
// };

Worker.prototype.stop = function() {
  if (this.running === false) { return q(); }
  this.running = false;
  
  // clearInterval(this.intervalId);
  // wait for workers to stop - or kill workers, configurable by a parameter
  return q();
};

// Worker.prototype.taskStats = function(worker) {
//   if (typeof(worker.handler.stats) !== 'function') {
//     return q({});
//   }
//
//   return worker.handler.stats();
// };

Worker.prototype.createChild = function() {
  var child = new ChildProcess(this.handlerPath, this.loggerProvider);
  child.executionCount = 0;
  child.on('heartbeat', this.onHeartbeat);
  
  return child;
};

Worker.prototype.destroyChild = function(child) {
  child.removeListener('heartbeat', this.onHeartbeat);
  return child.stop();
};

Worker.prototype.getChild = function(taskId, envelopeId) {
  var child = this.children.inactive.pop();
  child.taskId = taskId;
  child.envelopeId = envelopeId;
  this.children.active[taskId] = child;
  
  return child;
};

Worker.prototype.releaseChild = function(child) {
  var taskId = child.taskId;
  
  delete child.taskId;
  delete child.envelopeId;
  delete this.children.active[taskId];
  
  if (child.executionCount >= this.executionLimit) {
    // cycle out child and create a new one
    debug('Cycling out worker child after ' + child.executionCount + ' executions');
    var self = this;
    this.destroyChild(child).then(function() {
      child = self.createChild();
      return child.start();
    }).then(function() {
      self.children.inactive.unshift(child);
      setImmediate(self._cycle.bind(self));
    });
  } else {
    this.children.inactive.unshift(child);
  }
};

Worker.prototype.startTask = function(envelope) {
  var now = Date.now();
  
  var id = envelope.id;
  var task = envelope.task;
  task.execution = now;
  
  var child = this.getChild(task.id, envelope.id);
  ++child.executionCount;
  
  // setup logging, record times, start kill timer, etc.
  this.taskData[task.id] = {
    execution: now,
    startedAt: now,
    stats: []
  };
  
  if (this.storageProvider) {
    this.storageProvider.get(task.id, now).start({
      queue: this.queue.name,
      data: task.data,
      queuedAt: task.ts,
      startedAt: now,
      tags: task.tags || []
    });
  }
  
  debug([
    'Task ' + task.id + ' [' + this.queue.name + ']',
    '  - Queued : ' + moment(task.ts).format(),
    '  - Started: ' + moment(now).format() + ' (' + duration(now - task.ts) + ')'
  ].join('\n'));
  
  return child;
};

Worker.prototype.finishTask = function(envelope, err) {
  var now = Date.now();
  var task = envelope.task;
  var taskData = this.taskData[task.id];
  var child = this.children.active[task.id];
  
  this.releaseChild(child);
  
  taskData.finishedAt = now;
  taskData.success = !!!err;
  taskData.error = err;
  
  if (this.storageProvider) {
    this.storageProvider.get(task.id, taskData.execution).finish({
      finishedAt: taskData.finishedAt,
      success: taskData.success,
      error: taskData.error ? taskData.error.stack : undefined,
      env: process.env
    });
  }
  
  debug([
    'Task ' + task.id + ' [' + this.queue.name + ']',
    '  - Queued  : ' + moment(task.ts).format(),
    '  - Started : ' + moment(taskData.startedAt).format() + ' (' + duration(taskData.startedAt - task.ts) + ')',
    '  - Finished: ' + moment(now).format() + ' (' + duration(now - taskData.startedAt) + ')',
    '  - Success : ' + (taskData.success ? 'YES' : 'NO')
  ].join('\n'));
};

Worker.prototype.executeTask = function(envelope) {
  var self = this;
  var worker = this.startTask(envelope);
  
  return worker.execute(envelope.task.data, envelope.task).then(function() {
    return self.finishTask(envelope);
  }).catch(function(err) {
    debug('Worker Error:');
    err.stack.split('\n').map(function(line) {
      return debug('Worker Error:', line);
    });
    debug('Worker Error:');
    
    return self.finishTask(envelope, err);
  }).then(function() {
    return self.queue.complete(envelope.id);
  }).then(function() {
    
  });
};

Worker.prototype._cycle = function() {
  if (this.running === false) { return; }
  if (this.children.inactive.length === 0) { return; }
  if (this._popping === true) { return; }
  
  this._popping = true;
  
  var self = this;
  
  this.queue.pop().then(function(envelope) {
    self.executeTask(envelope).catch(function(err) {
      console.log(err.stack);
    }).finally(function() {
      setImmediate(self._cycle.bind(self));
    });
  }).catch(function(err) {
    console.log(err.stack);
  }).finally(function() {
    self._popping = false;
    setImmediate(self._cycle.bind(self));
  });
};

module.exports = Worker;
