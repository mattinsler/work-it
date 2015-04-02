var q = require('q');
var fs = require('fs');
var spawn = require('child_process').spawn;
var LineStream = require('byline').LineStream;
var PassThrough = require('stream').PassThrough;
var EventEmitter = require('events').EventEmitter;

var NODE_PATH = process.execPath;
var CHILD_PATH = require.resolve('./worker-child.js');

var ChildProcess = function(processPath, loggerProvider) {
  this.path = processPath;
  this.loggerProvider = loggerProvider;
  
  this.logStream = new PassThrough().pipe(new LineStream({keepEmptyLines: true}));
  
  this.deferred = {};
  
  this.events = {
    log: this.log.bind(this),
    
    message: function(message) {
      if (message.type === 'ready') {
        this.deferred.started.resolve();
      } else if (message.type === 'heartbeat') {
        this.emit('heartbeat', this);
      } else if (message.type === 'success') {
        this.finishExecution(null, message.success);
      } else if (message.type === 'failure') {
        var err = new Error(message.failure.message);
        err.name = message.failure.name;
        err.stack = message.failure.stack || message.failure.message;
        this.finishExecution(err);
      } else if (message.type === 'error') {
        var err = new Error('Uncaught Exception: ' + message.error.message);
        err.name = message.error.name;
        err.stack = message.error.stack || message.error.message;
        this.finishExecution(err);
      }
    }.bind(this),
    
    exit: function() {
      if (this.deferred.stopped) {
        return this.deferred.stopped.resolve();
      }
      
      // crap, this shouldn't happen
      this.finishExecution(new Error('Process exited unexpectedly'));
    }.bind(this)
  };
  
  this.__defineGetter__('active', function() {
    return !!this.current;
  });
  
  EventEmitter.call(this);
};

ChildProcess.prototype.__proto__ = EventEmitter.prototype;

ChildProcess.prototype.start = function() {
  if (!this.deferred.started) {
    this.deferred.started = q.defer();
    
    this.proc = spawn(NODE_PATH, [CHILD_PATH], {
      cwd: process.cwd(),
      env: process.env,
      stdio: ['pipe', 'pipe', 'pipe', 'ipc']
    });
    
    this.proc.stdout.pipe(this.logStream);
    this.proc.stderr.pipe(this.logStream);
    
    this.logStream.on('data', this.events.log);
    
    this.proc.on('message', this.events.message);
    this.proc.on('exit', this.events.exit);
    
    this.proc.send({
      type: 'init',
      init: this.path
    });
  }
  
  return this.deferred.started.promise;
};

ChildProcess.prototype.stop = function() {
  if (!this.deferred.stopped) {
    this.deferred.stopped = q.defer();
    
    // this.proc.removeListener('exit', this.events.exit);
    this.proc.removeListener('message', this.events.message);
    
    this.logStream.removeListener('data', this.events.log);
    
    this.proc.kill();
  }
  
  return this.deferred.stopped.promise;
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
