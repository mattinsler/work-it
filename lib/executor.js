var q = require('q');
var fs = require('fs');
var spawn = require('child_process').spawn;
var LineStream = require('byline').LineStream;

var NODE_PATH = process.execPath;
var CHILD_PATH = require.resolve('./worker-child.js');

exports.create = function(handlerPath, opts) {
  if (!fs.existsSync(handlerPath)) {
    throw new Error('Handler file does not exist: ' + handlerPath);
  }
  
  var logProvider = opts && opts.logProvider ? opts.logProvider : require('./console-log-provider')();
  
  return function(data, envelope) {
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
};
