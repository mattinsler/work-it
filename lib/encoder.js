var msgpack = require('msgpack');

/** Encodes object to string */
exports.encode = function(obj) {
  return msgpack.pack(obj).toString('hex');
};

/** Decodes string to object */
exports.decode = function(str) {
  return msgpack.unpack(Buffer(str, 'hex'));
};

/** Decodes message popped from a queue */
exports.decodeMessage = function(message) {
  var match = /^([^|]*)\|([^|]*)\|([^|]*)\|([^|]+)\|(.*)$/.exec(message);
  
  return {
    id: message,
    popper: match[1],
    retryQueue: match[2],
    failedCount: parseInt(match[3]),
    reapedCount: parseInt(match[4]),
    task: exports.decode(match[5])
  };
};

exports.encodeMessage = function(opts) {
  var message;
  if (opts.popper && opts.retryQueue) {
    message = [
      opts.popper,
      opts.retryQueue,
      opts.failedCount,
      opts.reapedCount,
      exports.encode(task)
    ].join('|');
  } else {
    message = [
      opts.failedCount,
      opts.reapedCount,
      exports.encode(opts.task)
    ].join('|');
  }
  
  return message;
};
