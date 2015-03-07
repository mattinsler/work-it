var msgpack = require('msgpack');

/** Encodes object to string */
exports.encode = function(obj) {
  return msgpack.pack(obj).toString('hex');
};

/** Decodes string to object */
exports.decode = function(str) {
  return msgpack.unpack(Buffer(str, 'hex'));
};
