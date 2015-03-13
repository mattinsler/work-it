var q = require('q');
var fs = require('fs');
var spawn = require('child_process').spawn;
var LineStream = require('byline').LineStream;

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

exports.create = function(handlerPath, opts) {
  if (!fs.existsSync(handlerPath)) {
    throw new Error('Handler file does not exist: ' + handlerPath);
  }
  
  var logProvider = opts && opts.logProvider ? opts.logProvider : require('./console-log-provider')();
  
  var handler = function(data, envelope) {
    var d = q.defer();
    
    var logger = logProvider.get(envelope.id, envelope.execution);
    
    var proc = spawn('/bin/sh', ['-c', NODE_PATH + ' ' + CHILD_PATH], {
      cwd: process.cwd(),
      env: process.env,
      stdio: ['pipe', 'pipe', 'pipe', 'ipc']
    });
    
    proc.stdout.pipe(new LineStream({keepEmptyLines: true})).on('data', logger.log.bind(logger));
    proc.stderr.pipe(new LineStream({keepEmptyLines: true})).on('data', logger.log.bind(logger));
    
    proc.on('message', function(result) {
      proc.kill();
      
      logger.end().then(function(logs) {
        if (!!logs) { envelope.logs = logs; }
        
        if (result.success) {
          d.resolve();
        } else {
          var err = new Error(result.error.message);
          err.name = result.error.name;
          err.stack = result.error.stack;
          d.reject(err);
        }
      }).catch(function(err) {
        console.log(err.stack);
      });
    });
  
    proc.send({
      handler: handlerPath,
      data: data,
      envelope: envelope
    });
    
    return d.promise;
  };
  
  handler.path = handlerPath;
  
  handler.stats = function() {
    var d = q.defer();
    var procFile = '/proc/' + proc.pid + '/stat';
    
    fs.exists(procFile, function(exists) {
      if (exists) {
        fs.readFile(procFile, function(err, buffer) {
          if (err) { return d.reject(err); }
          var stats = {};
          var parts = buffer.toString().trim().split(' ');
          for (var x = 0; x < PROC_STAT.length; ++x) {
            var v = parts[x];
            if (v.toString() === parseInt(v).toString()) { v = parseInt(v); }
            stats[PROC_STAT[x]] = v;
          }
          statsDeferred.resolve(stats);
        });
      } else {
        statsDeferred.resolve({});
      }
    });
    
    return statsDeferred.promise;
  };
  
  return handler;
};
