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
  
  this.machineId = opts.machineId || require('os').hostname() + '.' + process.pid;
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
  
  this.onHeartbeat = function(child) {
    if (child.taskId) {
      this.queue.heartbeat(child.taskId);
    }
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

Worker.prototype.getChild = function(taskId) {
  var child = this.children.inactive.pop();
  child.taskId = taskId;
  this.children.active[taskId] = child;
  
  return child;
};

Worker.prototype.releaseChild = function(child) {
  if (!child) { return; }
  
  var taskId = child.taskId;
  
  delete child.taskId;
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

Worker.prototype.startTask = function(task) {
  var self = this;
  var now = new Date();
  var child;
  
  return q().then(function() {
    task.execution = now.getTime();
    task.startedAt = now;
    
    if (!task.failedCount) { task.failedCount = 0; }
    if (!task.reapedCount) { task.reapedCount = 0; }
  
    child = self.getChild(task.id);
    ++child.executionCount;
    
    debug([
      'Task ' + task.id + ' [' + self.queue.name + ']',
      '  - Queued : ' + moment(task.queuedAt).format(),
      '  - Started: ' + moment(now).format() + ' (' + duration(now - task.queuedAt) + ')',
      '  - Failed : ' + task.failedCount,
      '  - Reaped : ' + task.reapedCount
    ].join('\n'));
    
    if (self.storageProvider) {
      return self.storageProvider.get(task.id, task.execution).start({
        queue: task.queue,
        retryQueue: task.retryQueue,
        queuedAt: task.queuedAt,
        startedAt: task.startedAt,
        data: JSON.stringify(task.data),
        tags: task.tags,
        failedCount: task.failedCount,
        reapedCount: task.reapedCount
      });
    }
  }).then(function() {
    return child;
  });
};

Worker.prototype.finishTask = function(task, err) {
  var self = this;
  
  return q().then(function() {
    var now = new Date();
    var child = self.children.active[task.id];
  
    self.releaseChild(child);
  
    task.finishedAt = now;
    task.success = !!!err;
    task.error = err;
  
    debug([
      'Task ' + task.id + ' [' + task.queue + ']',
      '  - Queued  : ' + moment(task.queuedAt).format(),
      '  - Started : ' + moment(task.startedAt).format() + ' (' + duration(task.startedAt - task.queuedAt) + ')',
      '  - Finished: ' + moment(now).format() + ' (' + duration(now - task.startedAt) + ')',
      '  - Success : ' + (task.success ? 'YES' : 'NO')
    ].join('\n'));
  
    if (self.storageProvider) {
      return self.storageProvider.get(task.id, task.execution).finish({
        finishedAt: task.finishedAt,
        success: task.success,
        error: task.error ? task.error.stack : undefined,
        env: JSON.stringify(process.env)
      });
    }
  });
};

Worker.prototype.executeTask = function(task) {
  var self = this;
  
  return this.startTask(task).then(function(worker) {
    return worker.execute(task.data, task).then(function() {
      return self.finishTask(task).then(function() {
        return self.queue.complete(task.id);
      });
    }).catch(function(err) {
      debug('Worker Error:');
      err.stack.split('\n').map(function(line) {
        return debug('Worker Error:', line);
      });
      debug('Worker Error:');
      
      return self.finishTask(task, err).then(function() {
        return self.queue.fail(task.id);
      });
    });
  });
};

Worker.prototype._cycle = function() {
  if (this.running === false) { return; }
  if (this.children.inactive.length === 0) { return; }
  if (this._popping === true) { return; }
  
  this._popping = true;
  
  var self = this;
  
  this.queue.pop(this.machineId).then(function(task) {
    self.executeTask(task).catch(function(err) {
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
