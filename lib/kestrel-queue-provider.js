var q = require('q');
var encoder = require('./encoder');
var Kestrel = require('kestrel.node');

var KestrelQueue = function(provider, name, opts) {
  this.name = name;
  this.opts = opts || {};
  if (!this.opts.timeout) { this.opts.timeout = 5000; }
  
  this.poppers = [];
  this._popClients = [];
  
  this._nextMessageID = 1;
  this._messageIDClientMap = {};
  
  this.__defineGetter__('pushClient', function() {
    return provider.getPushClient().queue(this.name);
  });
  this.__defineGetter__('popClient', function() {
    if (this._popClients.length === 0) {
      this._popClients.push(provider.createConnection().queue(this.name));
    }
    return this._popClients.pop();
  });
};

KestrelQueue.prototype.push = function(message) {
  return this.pushClient.set(encoder.encode(message));
};

KestrelQueue.prototype.pop = function() {
  var d = q.defer();
  
  this.poppers.unshift(d);
  setImmediate(this._churnPop.bind(this));
  
  return d.promise;
};

KestrelQueue.prototype.complete = function(messageID) {
  var self = this;
  var client = this._messageIDClientMap[messageID];
  delete this._messageIDClientMap[messageID];
  
  if (!client) { return q(); }
  return client.close().then(function() {
    // return client to pool
    self._popClients.push(client);
  });
};

KestrelQueue.prototype.abort = function(messageID) {
  var self = this;
  var client = this._messageIDClientMap[messageID];
  delete this._messageIDClientMap[messageID];
  
  if (!client) { return q(); }
  return client.abort().then(function() {
    // return client to pool
    self._popClients.push(client);
  });
};

KestrelQueue.prototype._churnPop = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var client = this.popClient.open(this.opts.timeout);
  client.then(function(message) {
    if (message) {
      var popper = self.poppers.pop();
      
      if (popper) {
        var messageID = self._nextMessageID++;
        
        self._messageIDClientMap[messageID] = client;
        
        popper.resolve({
          id: messageID,
          task: encoder.decode(message)
        });
      } else {
        // oops, somehow we asked for too many messages... abort! abort!
        client.abort();
        // return client to pool
        self._popClients.push(client);
      }
    }
    
    setImmediate(self._churnPop.bind(self));
  }).catch(function(err) {
    console.log(err.stack);
  });
};





var KestrelQueueProvider = function(servers) {
  if (!(this instanceof KestrelQueueProvider)) {
    return new KestrelQueueProvider(servers);
  }
  
  if (servers.length > 1) {
    throw new Error('KestrelQueueProvider currently only supports a single server')
  }
  
  this.servers = servers;
  this.queueMap = {};
};

KestrelQueueProvider.prototype.get = function(name, opts) {
  if (!this.queueMap[name]) {
    this.queueMap[name] = new KestrelQueue(this, name, opts);
  }
  
  return this.queueMap[name];
};

KestrelQueueProvider.prototype.getQueueNames = function() {
  return this.getPushClient().stats().then(function(stats) {
    return Object.keys(stats.queue);
  });
};

KestrelQueueProvider.prototype.getQueueStats = function() {
  return this.getPushClient().stats().then(function(stats) {
    return Object.keys(stats.queue).map(function(name) {
      stats.queue[name].name = name;
      return stats.queue[name];
    });
  });
};

KestrelQueueProvider.prototype.createConnection = function() {
  return new Kestrel(this.servers[0]);
};

KestrelQueueProvider.prototype.getPushClient = function() {
  if (!this.pushClient) {
    this.pushClient = this.createConnection();
  }
  return this.pushClient;
};

module.exports = KestrelQueueProvider;
