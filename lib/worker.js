var q = require('q');
var moment = require('moment');

var duration = function(millis) {
  var d = moment.duration(millis);
  return d.minutes() + 'm ' + d.seconds() + 's ' + d.milliseconds() + 'ms';
};

var Worker = function(opts) {
  if (!(this instanceof Worker)) {
    return new Worker(opts);
  }
  
  if (!opts) { opts = {}; }
  if (!opts.command) { throw new Error('You must configure a Worker with a command'); }
  if (!opts.queueProvider) { throw new Error('You must configure a Worker with a queue provider'); }
  if (!opts.handler) { throw new Error('You must configure a worker with a handler'); }
  
  this.command = opts.command;
  this.queueProvider = opts.queueProvider;
  this.statusProvider = opts.statusProvider;
  this.handler = opts.handler;
  this.concurrency = opts.concurrency || 1;
  
  this.running = false;
  this.workers = {};
  this.__defineGetter__('activeWorkers', function() {
    return Object.keys(this.workers).length;
  });
};

Worker.prototype.start = function() {
  if (!this.running) {
    this.running = true;
    
    console.log();
    console.log('Worker configuration:');
    console.log('  - Command    :', this.command);
    console.log('  - Handler    :', this.handler.path);
    console.log('  - Concurrency:', this.concurrency);
    console.log();
    
    setImmediate(this._cycle.bind(this));
    
    var self = this;
    this.intervalId = setInterval(function() {
      q.all(
        Object.keys(self.workers).map(function(key) {
          var worker = self.workers[key];
          return self.taskStats(worker).then(function(stats) {
            stats.ts = Date.now() - worker.startedAt;
            worker.stats.push(stats);
            console.log([
              'ts : ' + stats.ts,
              'cpu: ' + stats.cpu.user + '/' + stats.cpu.system,
              'mem: ' + stats.mem.resident + '/' + stats.mem.virtual
            ].join('\n'));
          });
        })
      )
    }, 2500);
  }
  return q();
};

Worker.prototype.stop = function() {
  if (!this.running) { return q(); }
  
  clearInterval(this.intervalId);
  this.running = false;
  // wait for workers to stop - or kill workers, configurable by a parameter
  return q();
};

Worker.prototype.taskStats = function(worker) {
  if (typeof(worker.handler.stats) !== 'function') {
    return q({});
  }
  
  return worker.handler.stats();
};

Worker.prototype._startTask = function(taskEnvelope) {
  var now = Date.now();
  var task = taskEnvelope.task;
  task.execution = now;
  
  // setup logging, record times, start kill timer, etc.
  this.workers[task.id] = {
    execution: now,
    startedAt: now,
    stats: []
  };
  
  if (this.statusProvider) {
    this.statusProvider.get(task.id, now).start({
      command: this.command,
      data: task.data,
      queuedAt: task.ts,
      startedAt: now
    });
  }
  
  console.log([
    'Task ' + task.id + ' [' + this.command + ']',
    '  - Queued : ' + moment(task.ts).format(),
    '  - Started: ' + moment(now).format() + ' (' + duration(now - task.ts) + ')'
  ].join('\n'));
  
  return this.workers[task.id];
};

Worker.prototype._finishTask = function(taskEnvelope, err) {
  var now = Date.now();
  var task = taskEnvelope.task;
  var worker = this.workers[task.id];
  
  worker.finishedAt = now;
  worker.success = !!!err;
  worker.error = err;
  
  if (this.statusProvider) {
    this.statusProvider.get(task.id, worker.execution).finish({
      finishedAt: worker.finishedAt,
      success: worker.success,
      error: worker.error ? worker.error.stack : undefined,
      env: process.env,
      logs: task.logs ? task.logs : undefined
    });
  }
  
  console.log([
    'Task ' + task.id + ' [' + this.command + ']',
    '  - Queued  : ' + moment(task.ts).format(),
    '  - Started : ' + moment(worker.startedAt).format() + ' (' + duration(worker.startedAt - task.ts) + ')',
    '  - Finished: ' + moment(now).format() + ' (' + duration(now - worker.startedAt) + ')',
    '  - Success : ' + (worker.success ? 'YES' : 'NO')
  ].join('\n'));
  
  delete this.workers[task.id];
};

Worker.prototype._cycle = function() {
  if (!this.running) { return; }
  if (this.activeWorkers === this.concurrency) { return; }
  
  var self = this;
  var queue = this.queueProvider.get(this.command);
  
  queue.pop().then(function(taskEnvelope) {
    var task = taskEnvelope.task;
    var messageID = taskEnvelope.id;
    
    var worker = self._startTask(taskEnvelope);
    setImmediate(self._cycle.bind(self));
    
    worker.handler = self.handler(task.data, task);
    
    return q.when(worker.handler).then(function() {
      self._finishTask(taskEnvelope);
      queue.complete(messageID);
    }).catch(function(err) {
      console.log('Worker Error:');
      err.stack.split('\n').map(function(line) {
        return console.log('Worker Error:', line);
      });
      console.log('Worker Error:');
      
      self._finishTask(taskEnvelope, err);
      // queue.abort(messageID);
      queue.complete(messageID);
    });
  }).catch(function(err) {
    console.log(err.stack);
  }).finally(function() {
    setImmediate(self._cycle.bind(self));
  });
};

module.exports = Worker;
