var q = require('q');
var fs = require('fs');
var spawn = require('child_process').spawn;
var LineStream = require('byline').LineStream;
var EventEmitter = require('events').EventEmitter;

var NODE_PATH = process.execPath;
var CHILD_PATH = require.resolve('./worker-child.js');

var ChildProcess = function(processPath) {
  this.path = processPath;
  
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
  
  this.proc.stdout.pipe(process.stdout);
  this.proc.stderr.pipe(process.stderr);
  
  this.proc.on('message', function(message) {
    // console.log(message);
    
    if (message.type === 'ready') {
      d.resolve();
    } else if (message.type === 'heartbeat') {
      self.emit('heartbeat', self);
    } else if (message.type === 'success') {
      self.current.deferred.resolve(message.success);
      delete self.current;
    } else if (message.type === 'failure') {
      var err = new Error(message.failure.message);
      err.name = message.failure.name;
      err.stack = message.failure.stack || message.failure.message;
      self.current.deferred.reject(err);
      delete self.current;
    } else if (message.type === 'error') {
      var err = new Error('Uncaught Exception: ' + message.error.message);
      err.name = message.error.name;
      err.stack = message.error.stack || message.error.message;
      self.current.deferred.reject(err);
      delete self.current;
    }
  });
  
  this.proc.on('exit', function() {
    // crap, this shouldn't happen
    
    // check if you still have a current and error out
    if (self.current) {
      self.current.deferred.reject(new Error('Process exited unexpectedly'));
      delete self.current;
    }
    // finish logger
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

ChildProcess.prototype.execute = function(data, envelope) {
  if (this.current) { throw new Error('Already executing a command in this process'); }
  
  this.current = {
    data: data,
    envelope: envelope,
    // logger: ...
    deferred: q.defer()
  };
  
  this.proc.send({
    type: 'execute',
    execute: {
      data: data,
      envelope: envelope
    }
  });
  
  return this.current.deferred.promise;
};

module.exports = ChildProcess;
