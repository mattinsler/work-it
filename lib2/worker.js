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
  
  
  // if (!opts.command) { throw new Error('You must configure a Worker with a command'); }
  // if (!opts.queueProvider) { throw new Error('You must configure a Worker with a queue provider'); }
  
  //
  // this.command = opts.command;
  // this.monitoring = opts.monitoring;
  // this.queueProvider = opts.queueProvider;
  // this.statusProvider = opts.statusProvider;
  // this.handler = opts.handler;
  // this.concurrency = opts.concurrency || 1;
  
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
    console.log('Got heartbeat from worker for task', worker.taskId);
    this.queue.heartbeat(worker.envelopeId);
  }.bind(this);
  
  for (var x = 0; x < this.concurrency; ++x) {
    var proc = new ChildProcess(this.handlerPath);
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

// Worker.prototype._startTask = function(taskEnvelope) {
//   var now = Date.now();
//   var task = taskEnvelope.task;
//   task.execution = now;
//
//   // setup logging, record times, start kill timer, etc.
//   this.workers[task.id] = {
//     execution: now,
//     startedAt: now,
//     stats: []
//   };
//
//   if (this.statusProvider) {
//     this.statusProvider.get(task.id, now).start({
//       command: this.command,
//       data: task.data,
//       queuedAt: task.ts,
//       startedAt: now
//     });
//   }
//
//   console.log([
//     'Task ' + task.id + ' [' + this.command + ']',
//     '  - Queued : ' + moment(task.ts).format(),
//     '  - Started: ' + moment(now).format() + ' (' + duration(now - task.ts) + ')'
//   ].join('\n'));
//
//   return this.workers[task.id];
// };
//
// Worker.prototype._finishTask = function(taskEnvelope, err) {
//   var now = Date.now();
//   var task = taskEnvelope.task;
//   var worker = this.workers[task.id];
//
//   worker.finishedAt = now;
//   worker.success = !!!err;
//   worker.error = err;
//
//   if (this.statusProvider) {
//     this.statusProvider.get(task.id, worker.execution).finish({
//       finishedAt: worker.finishedAt,
//       success: worker.success,
//       error: worker.error ? worker.error.stack : undefined,
//       env: process.env,
//       logs: task.logs ? task.logs : undefined
//     });
//   }
//
//   console.log([
//     'Task ' + task.id + ' [' + this.command + ']',
//     '  - Queued  : ' + moment(task.ts).format(),
//     '  - Started : ' + moment(worker.startedAt).format() + ' (' + duration(worker.startedAt - task.ts) + ')',
//     '  - Finished: ' + moment(now).format() + ' (' + duration(now - worker.startedAt) + ')',
//     '  - Success : ' + (worker.success ? 'YES' : 'NO')
//   ].join('\n'));
//
//   delete this.workers[task.id];
// };

// Worker.prototype._executeTask = function(taskEnvelope) {
//   var task = taskEnvelope.task;
//
//   var self = this;
//   var worker = this._startTask(taskEnvelope);
//   worker.handler = this.handler(task.data, task);
//   worker.handler.on('heartbeat', function() {
//     console.log('HEARTBEAT', task.id);
//     self.beat(task.id);
//   });
//
//   return q.when(worker.handler.execute()).then(function() {
//     return self._finishTask(taskEnvelope);
//   }).catch(function(err) {
//     console.log('Worker Error:');
//     err.stack.split('\n').map(function(line) {
//       return console.log('Worker Error:', line);
//     });
//     console.log('Worker Error:');
//
//     // nerfs error reponse to always complete job and not abort it
//     return self._finishTask(taskEnvelope, err);
//   });
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
  
  // save status to mongodb
  // if (this.statusProvider) {
  //   this.statusProvider.get(task.id, now).start({
  //     command: this.command,
  //     data: task.data,
  //     queuedAt: task.ts,
  //     startedAt: now
  //   });
  // }
  
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
  
  // save to mongodb
  // if (this.statusProvider) {
  //   this.statusProvider.get(task.id, worker.execution).finish({
  //     finishedAt: worker.finishedAt,
  //     success: worker.success,
  //     error: worker.error ? worker.error.stack : undefined,
  //     env: process.env,
  //     logs: task.logs ? task.logs : undefined
  //   });
  // }

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
  
  var self = this;
  
  this.queue.pop().then(function(envelope) {
    return self.executeTask(envelope);
  }).catch(function(err) {
    console.log(err.stack);
  }).finally(function() {
    setImmediate(self._cycle.bind(self));
  });
};

module.exports = Worker;
