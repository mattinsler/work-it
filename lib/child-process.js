var q = require('q');
var fs = require('fs');
var spawn = require('child_process').spawn;
var LineStream = require('byline').LineStream;
var EventEmitter = require('events').EventEmitter;

var NODE_PATH = process.execPath;
var CHILD_PATH = require.resolve('./worker-child.js');

var ChildProcess = function(processPath, loggerProvider) {
  this.path = processPath;
  this.loggerProvider = loggerProvider;
  
  this.__defineGetter__('active', function() {
    return !!this.current;
  });
  
  EventEmitter.call(this);
};

ChildProcess.prototype.__proto__ = EventEmitter.prototype;

ChildProcess.prototype.start = function() {
  var self = this;
  var d = q.defer();
  
  this.proc = spawn(NODE_PATH, [CHILD_PATH], {
    cwd: process.cwd(),
    env: process.env,
    stdio: ['pipe', 'pipe', 'pipe', 'ipc']
  });
  
  this.proc.stdout.pipe(new LineStream({keepEmptyLines: true})).on('data', this.log.bind(this));
  this.proc.stderr.pipe(new LineStream({keepEmptyLines: true})).on('data', this.log.bind(this));
  
  this.proc.on('message', function(message) {
    // console.log(message);
    
    if (message.type === 'ready') {
      d.resolve();
    } else if (message.type === 'heartbeat') {
      self.emit('heartbeat', self);
    } else if (message.type === 'success') {
      self.finishExecution(null, message.success);
    } else if (message.type === 'failure') {
      var err = new Error(message.failure.message);
      err.name = message.failure.name;
      err.stack = message.failure.stack || message.failure.message;
      self.finishExecution(err);
    } else if (message.type === 'error') {
      var err = new Error('Uncaught Exception: ' + message.error.message);
      err.name = message.error.name;
      err.stack = message.error.stack || message.error.message;
      self.finishExecution(err);
    }
  });
  
  this.proc.on('exit', function() {
    // crap, this shouldn't happen
    self.finishExecution(new Error('Process exited unexpectedly'));
  });
  
  this.proc.send({
    type: 'init',
    init: this.path
  });
  
  return d.promise;
};

ChildProcess.prototype.stop = function() {
  this.proc.kill();
};

ChildProcess.prototype.startExecution = function(data, envelope) {
  this.current = {
    data: data,
    envelope: envelope,
    logger: this.loggerProvider.get(envelope.id, envelope.execution),
    deferred: q.defer()
  };
};

ChildProcess.prototype.finishExecution = function(err, data) {
  if (!this.current) {
    if (err) { console.log(err.stack); }
    return;
  }
  
  var self = this;
  
  this.current.logger.end().then(function() {
    if (err) {
      self.current.deferred.reject(err);
    } else {
      self.current.deferred.resolve(data);
    }
    delete self.current;
  });
};

ChildProcess.prototype.execute = function(data, envelope) {
  if (this.current) { throw new Error('Already executing a command in this process'); }
  
  this.startExecution(data, envelope);
  
  this.proc.send({
    type: 'execute',
    execute: {
      data: data,
      envelope: envelope
    }
  });
  
  return this.current.deferred.promise;
};

ChildProcess.prototype.log = function(message) {
  if (this.current && this.current.logger) {
    this.current.logger.log(message);
  }
};

module.exports = ChildProcess;
