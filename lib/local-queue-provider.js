var q = require('q');

var Queue = function() {
  this.queue = [];
  this.waiters = [];
};

Queue.prototype.push = function(message) {
  this.queue.unshift(message);
  setImmediate(this._churn.bind(this));
  return q(this);
};

Queue.prototype.pop = function() {
  var d = q.defer();
  this.waiters.unshift(d);
  setImmediate(this._churn.bind(this));
  return d.promise;
};

Queue.prototype._churn = function() {
  if (this.queue.length === 0 || this.waiters.length === 0) { return; }
  this.waiters.pop().resolve(this.queue.pop());
};




var LocalQueueProvider = function() {
  this.queueMap = {};
};

LocalQueueProvider.prototype.get = function(name, opts) {
  if (!this.queueMap[name]) {
    this.queueMap[name] = new Queue(opts);
  }
  return this.queueMap[name];
};

module.exports = LocalQueueProvider;
