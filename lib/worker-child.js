var q = require('q');

var formatError = function(err) {
  return {
    name: err.name,
    message: err.message,
    stack: err.stack
  };
};

process.on('uncaughtException', function(err) {
  process.send({
    type: 'error',
    error: formatError(err)
  });
});

var heartbeat = function() {
  process.send({
    type: 'heartbeat'
  });
};

var handler;

process.on('message', function(message) {
  if (message.type === 'init') {
    q().then(function() {
      handler = require(message.init);
      
      if (typeof(handler.init) === 'function') {
        return q.when(handler.init());
      }
    }).then(function() {
      process.send({
        type: 'ready'
      });
      setInterval(heartbeat, 1000);
    }).catch(function(err) {
      process.send({
        type: 'error',
        error: formatError(err)
      });
    });
  } else if (message.type === 'execute') {
    q.when(handler.execute(message.execute.data, message.execute.envelope)).then(function(data) {
      process.send({
        type: 'success',
        success: data
      })
    }).catch(function(err) {
      process.send({
        type: 'failure',
        failure: formatError(err)
      });
    });
  }
});
