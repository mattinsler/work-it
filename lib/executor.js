var q = require('q');
var fs = require('fs');
var spawn = require('child_process').spawn;
var LineStream = require('byline').LineStream;
var EventEmitter = require('events').EventEmitter;

var NODE_PATH = process.execPath;
var CHILD_PATH = require.resolve('./worker-child.js');

var PROC_STAT = [
  'pid',
  'tcomm',
  'state',
  'ppid',
  'pgid',
  'sid',
  'tty_nr',
  'tty_pgrp',
  'flags',
  'min_flt',
  'cmin_flt',
  'maj_flt',
  'cmaj_flt',
  'utime',
  'stime',
  'cutime',
  'cstime',
  'priority',
  'nice',
  'num_threads',
  'it_real_value',
  'start_time',
  'vsize',
  'rss',
  'rsslim',
  'start_code',
  'end_code',
  'start_stack',
  'esp',
  'eip',
  'pending',
  'blocked',
  'sigign',
  'sigcatch',
  'wchan',
  'zero1',
  'zero2',
  'exit_signal',
  'cpu',
  'rt_priority',
  'policy'
];

var Executor = function(handlerPath, logProvider) {
  this.path = handlerPath;
  this.logProvider = logProvider;
  
  EventEmitter.call(this);
};

Executor.prototype.spawn = function() {
  this.proc = spawn(NODE_PATH, [CHILD_PATH], {
    cwd: process.cwd(),
    env: process.env,
    stdio: ['pipe', 'pipe', 'pipe', 'ipc']
  });
};

Executor.prototype.start = function(data, envelope) {
  var d = q.defer();
  var self = this;
  
  var result;
  
  this.proc.on('message', function(message) {
    if (message.heartbeat === true) {
      self.emit('heartbeat');
    } else if (message.success !== null && message.success !== undefined) {
      result = message;
    }
  });
  
  this.proc.on('exit', function() {
    self.logger.end().then(function(logs) {
      if (!!logs) { envelope.logs = logs; }
    
      if (result) {
        if (result.success) {
          d.resolve();
        } else {
          var err = new Error(result.error.message);
          err.name = result.error.name;
          err.stack = result.error.stack || result.error.message;
          d.reject(err);
        }
      } else {
        console.log('DOES NOT HAVE A PROC RESULT BEFORE PROC EXIT');
        d.reject(new Error('Process exited unexpectedly'));
      }
    });
  });
  
  this.proc.send({
    handler: this.path,
    data: data,
    envelope: envelope
  });
  
  return d.promise;
};

Executor.prototype.execute = function(data, envelope) {
  this.logger = this.logProvider.get(envelope.id, envelope.execution);
  this.logger.log = this.logger.log.bind(this.logger);
  
  this.proc.stdout.pipe(new LineStream({keepEmptyLines: true})).on('data', this.logger.log);
  this.proc.stderr.pipe(new LineStream({keepEmptyLines: true})).on('data', this.logger.log);
  
  return this.start(data, envelope);
};

Executor.prototype.stats = function() {
  var d = q.defer();
  var procFile = '/proc/' + this.proc.pid + '/stat';
  
  fs.exists(procFile, function(exists) {
    if (!exists) { return d.resolve({}); }
    
    fs.readFile(procFile, function(err, buffer) {
      if (err) { return d.reject(err); }
      
      var stats = {};
      var parts = buffer.toString().trim().split(' ');
      
      for (var x = 0; x < PROC_STAT.length; ++x) {
        var v = parts[x];
        if (v.toString() === parseInt(v).toString()) { v = parseInt(v); }
        stats[PROC_STAT[x]] = v;
      }
      
      d.resolve({
        cpu: {
          user: stats.utime,
          system: stats.stime,
        },
        mem: {
          virtual: stats.vsize,
          resident: stats.rss
        }
      });
    });
  });

  return d.promise;
};



exports.create = function(handlerPath, opts) {
  if (!fs.existsSync(handlerPath)) {
    throw new Error('Handler file does not exist: ' + handlerPath);
  }
  
  var logProvider = opts && opts.logProvider ? opts.logProvider : require('./console-log-provider')();
  
  return new Executor(handlerPath, logProvider);
};
