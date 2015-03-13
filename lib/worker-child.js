var q = require(__dirname + '/../node_modules/q');

process.on('message', function(info) {
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
  });
});
