var q = require('q');
var moment = require('moment');
var ChildProcess = require('./child-process');

var duration = function(millis) {
  var d = moment.duration(millis);
  return d.minutes() + 'm ' + d.seconds() + 's ' + d.milliseconds() + 'ms';
};

var Worker = function(queue, opts) {
  this.queue = queue;
  
  if (!opts) { opts = {}; }
  if (!opts.handler) { throw new Error('You must configure a worker with a handler'); }
  
  this.concurrency = opts.concurrency || 1;
  this.handlerPath = opts.handler;
  this.loggerProvider = opts.loggerProvider;
  this.storageProvider = opts.storageProvider;
  
  // if (!opts.command) { throw new Error('You must configure a Worker with a command'); }
  // if (!opts.queueProvider) { throw new Error('You must configure a Worker with a queue provider'); }
  
  this.running = false;
  this.workers = {
    active: {},
    inactive: []
  };
  
  this.taskData = {};
};

Worker.prototype.start = function() {
  if (this.running === true) { return q(); }
  this.running = true;
  
  console.log();
  console.log('Worker configuration:');
  console.log('  - Command    :', this.queue.name);
  console.log('  - Handler    :', this.handlerPath);
  console.log('  - Concurrency:', this.concurrency);
  console.log();
  console.log('Starting ' + this.concurrency + ' workers...');
  
  this.onHeartbeat = function(worker) {
    if (!worker.taskId) { return; }
    this.queue.heartbeat(worker.envelopeId);
  }.bind(this);
  
  for (var x = 0; x < this.concurrency; ++x) {
    var proc = new ChildProcess(this.handlerPath, this.loggerProvider);
    proc.on('heartbeat', this.onHeartbeat);
    this.workers.inactive.push(proc);
  }
  
  var self = this;
  return q.all(
    this.workers.inactive.map(function(w) {
      return w.start();
    })
  ).then(function() {
    console.log('All ' + self.workers.inactive.length + ' workers started');
    console.log();
    
    setImmediate(self._cycle.bind(self));
  });
};

// Worker.prototype.monitorResources = function() {
//   var self = this;
//
//   q.all(
//     Object.keys(self.workers).map(function(key) {
//       var worker = self.workers[key];
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

Worker.prototype.startTask = function(envelope) {
  var now = Date.now();
  
  var id = envelope.id;
  var task = envelope.task;
  task.execution = now;
  
  var worker = this.workers.inactive.pop();
  worker.taskId = task.id;
  worker.envelopeId = envelope.id;
  this.workers.active[task.id] = worker;

  // setup logging, record times, start kill timer, etc.
  this.taskData[task.id] = {
    execution: now,
    startedAt: now,
    stats: []
  };
  
  this.storageProvider.get(task.id, now).start({
    queue: this.queue.name,
    data: task.data,
    queuedAt: task.ts,
    startedAt: now,
    tags: task.tags || []
  });
  
  console.log([
    'Task ' + task.id + ' [' + this.queue.name + ']',
    '  - Queued : ' + moment(task.ts).format(),
    '  - Started: ' + moment(now).format() + ' (' + duration(now - task.ts) + ')'
  ].join('\n'));
  
  return worker;
};

Worker.prototype.finishTask = function(envelope, err) {
  var now = Date.now();
  var task = envelope.task;
  var taskData = this.taskData[task.id];
  var worker = this.workers.active[task.id];
  
  delete worker.taskId;
  delete worker.envelopeId;
  this.workers.inactive.unshift(worker);
  delete this.workers.active[task.id];
  
  taskData.finishedAt = now;
  taskData.success = !!!err;
  taskData.error = err;
  
  this.storageProvider.get(task.id, taskData.execution).finish({
    finishedAt: taskData.finishedAt,
    success: taskData.success,
    error: taskData.error ? taskData.error.stack : undefined,
    env: process.env
  });
  
  console.log([
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
    
    console.log('Worker Error:');
    err.stack.split('\n').map(function(line) {
      return console.log('Worker Error:', line);
    });
    console.log('Worker Error:');
    
    return self.finishTask(envelope, err);
  }).finally(function() {
    return self.queue.complete(envelope.id);
  });
};

Worker.prototype._cycle = function() {
  if (this.running === false) { return; }
  if (this.workers.inactive.length === 0) { return; }
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
