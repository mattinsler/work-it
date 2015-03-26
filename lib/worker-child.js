var q = require('q');

var heartbeat = function() {
  process.send({
    heartbeat: true
  });
};

process.on('message', function(info) {
  heartbeat();
  setInterval(heartbeat, 1000);
  
  return q().then(function() {
    var handler = require(info.handler);
    
    return q.when(handler(info.data, info.envelope)).then(function() {
      process.send({
        success: true
      });
    });
  }).catch(function(err) {
    process.send({
      success: false,
      error: {
        name: err.name,
        message: err.message,
        stack: err.stack
      }
    });
  }).finally(function() {
    process.exit();
  });
});
