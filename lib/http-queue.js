var q = require('q');
var encoder = require('./encoder');
var Backoff = require('./backoff');
var superagent = require('superagent');

var HttpQueue = function(redisClient, httpConfig, opts) {
  this.httpConfig = httpConfig;
  
  this.name = opts.queue;
  this.workingSet = opts.workingSet;
  this.redisClient = redisClient;
  
  this.retryQueue = 'q:' + (opts.retry || this.name);
  
  this.poppers = [];
  
  this.backoff = new Backoff(this._cycle, this);
};

HttpQueue.prototype.push = function(message) {
  return q.reject(new Error('Cannot push to HTTP Queues'));
};

HttpQueue.prototype.pop = function(id) {
  var d = q.defer();
  
  this.poppers.unshift({
    id: id,
    deferred: d
  });
  this.backoff.immediate();
  
  return d.promise;
};

HttpQueue.prototype.complete = function(message) {
  return this.redisClient.zrem(this.workingSet, message);
};

HttpQueue.prototype.heartbeat = function(message) {
  return this.redisClient.zadd(this.workingSet, Date.now(), message);
};

HttpQueue.prototype._cycle = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var popper = this.poppers.pop();
  var request = superagent[this.httpConfig.method](this.httpConfig.url).set('Accept', 'application/json');
  
  if (this.httpConfig.headers) {
    Object.keys(this.httpConfig.headers).forEach(function(k) {
      request = request.set(k, self.httpConfig.headers[k]);
    });
  }
  
  if (this.httpConfig.query) {
    request = request.query(this.httpConfig.query);
  }
  
  if (popper.id) { request = request.set('X-Pop-ID', popper.id); }
  request = request.set('X-Retry-Queue', this.retryQueue);
  
  request.end(function(err, res) {
    if (err) {
      if ((!res || typeof(res.statusCode) !== 'number') && err.code !== 'ECONNREFUSED') {
        console.log(err.stack);
      }
      self.poppers.push(popper);
      return self.backoff.next();
    }
    
    if (res && typeof(res.statusCode) === 'number' && res.statusCode === 200) {
      var message = res.body;
      popper.deferred.resolve(encoder.decodeMessage(message));
      self.backoff.reset();
    } else {
      self.poppers.push(popper);
      self.backoff.next();
    }
  });
};

module.exports = HttpQueue;
