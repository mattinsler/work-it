var q = require('q');
var kestrel = require('node-kestrel');
var encoder = require('./encoder');

var KestrelQueue = function(client, name, opts) {
  this.client = client;
  this.name = name;
  this.opts = opts || {};
  if (!this.opts.timeout) { this.opts.timeout = 5000; }
  
  this.pushers = [];
  this.poppers = [];
  this._pushing = false;
  
  this.onMessage = function(message) {
    var popper = this.poppers.pop();
    if (popper) {
      popper.resolve(encoder.decode(message.data));
    } else {
      // abort message
    }
    
    setImmediate(this._churnPop.bind(this));
  }.bind(this);
  
  this.onEmpty = function() {
    setImmediate(this._churnPop.bind(this));
  }.bind(this);
  
  this.client.on('message', this.onMessage);
  this.client.on('empty', this.onEmpty);
};

KestrelQueue.prototype.push = function(message) {
  var d = q.defer();
  
  this.pushers.unshift({
    deferred: d,
    message: encoder.encode(message)
  });
  setImmediate(this._churnPush.bind(this));
  
  return d.promise;
};

KestrelQueue.prototype.pop = function() {
  var d = q.defer();
  
  this.poppers.unshift(d);
  setImmediate(this._churnPop.bind(this));
  
  return d.promise;
};

KestrelQueue.prototype._churnPop = function() {
  if (this.poppers.length === 0) { return; }
  this.client.get(this.name, this.opts.timeout);
};

KestrelQueue.prototype._churnPush = function() {
  if (this._pushing) { return; }
  if (this.pushers.length === 0) { return; }
  
  this._pushing = true;
  
  var self = this;
  var pusher = this.pushers.pop();
  
  this.client.set(this.name, pusher.message, 0, function(err) {
    self._pushing = false;
    if (err) { return pusher.deferred.reject(new Error(err)); }
    pusher.deferred.resolve(self);
    setImmediate(self._churnPush.bind(self));
  });
};




var KestrelQueueProvider = function(servers) {
  if (!(this instanceof KestrelQueueProvider)) {
    return new KestrelQueueProvider(servers);
  }
  
  this.servers = servers;
  this.queueMap = {};
};

KestrelQueueProvider.prototype.get = function(name, opts) {
  if (!this.queueMap[name]) {
    var client = new kestrel.kestrelClient({
      connectionType: kestrel.connectionType.ROUND_ROBIN,
      servers: this.servers
    });
    client.connect();
    
    this.queueMap[name] = new KestrelQueue(client, name, opts);
  }
  
  return this.queueMap[name];
};

module.exports = KestrelQueueProvider;
