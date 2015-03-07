var q = require('q');

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
  })
};

Worker.prototype.start = function() {
  if (this.running) { return; }
  this.running = true;
  setImmediate(this._cycle.bind(this));
};

Worker.prototype.stop = function() {
  if (this.running) {
    this.running = false;
    // wait for workers to stop - or kill workers, configurable by a parameter
  }
};

Worker.prototype._startTask = function(task) {
  var now = Date.now();
  task.execution = now;
  
  // setup logging, record times, start kill timer, etc.
  this.workers[task.id] = {
    execution: now,
    startedAt: now
  };
  
  if (this.statusProvider) {
    this.statusProvider.get(task.id, now).start({
      command: this.command,
      data: task.data,
      queuedAt: task.ts,
      startedAt: now
    });
  }
  
  console.log('Task %s started at %s', task.id, this.workers[task.id].startedAt);
};

Worker.prototype._finishTask = function(task, err) {
  var now = Date.now();
  var worker = this.workers[task.id];

  worker.finishedAt = now;
  worker.success = !!!err;
  worker.error = err;

  if (this.statusProvider) {
    this.statusProvider.get(task.id, worker.execution).finish({
      finishedAt: worker.finishedAt,
      success: worker.success,
      error: worker.error ? worker.error.stack : undefined,
      logs: task.logs ? task.logs : undefined
    });
  }
  
  console.log('Task %s took %dms to start and executed in %dms', task.id, worker.startedAt - task.ts, worker.finishedAt - worker.startedAt);

  delete this.workers[task.id];
};

Worker.prototype._cycle = function() {
  if (!this.running) { return; }
  if (this.activeWorkers === this.concurrency) { return; }
  
  var self = this;
  
  this.queueProvider.get(this.command).pop().then(function(task) {
    self._startTask(task);
    setImmediate(self._cycle.bind(self));
    
    return q.when(self.handler(task.data, task)).then(function() {
      self._finishTask(task);
    }).catch(function(err) {
      console.log(err.stack);
      self._finishTask(task, err);
    });
  }).finally(function() {
    setImmediate(self._cycle.bind(self));
  });
};

module.exports = Worker;
