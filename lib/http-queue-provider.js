var q = require('q');
var Backoff = require('./backoff');
var superagent = require('superagent');

var HttpQueue = function(provider, name, opts) {
  this.name = name;
  this.url = provider.url;
  
  this.poppers = [];
  
  this.backoff = new Backoff(this._churnPop, this);
};

HttpQueue.prototype.push = function(message) {
  return q.reject(new Error('Cannot push to HTTP Queues'));
};

HttpQueue.prototype.pop = function() {
  var d = q.defer();
  
  this.poppers.unshift(d);
  this.backoff.immediate();
  
  return d.promise;
};

HttpQueue.prototype._churnPop = function() {
  if (this.poppers.length === 0) { return; }
  
  var self = this;
  var request = superagent[this.url.method](this.url.url).set('Accept', 'application/json');
  
  if (this.url.headers) {
    Object.keys(this.url.headers).forEach(function(k) {
      request = request.set(k, self.url.headers[k]);
    });
  }
  
  request.end(function(err, res) {
    if (res && typeof(res.statusCode) === 'number') {
      if (res.statusCode === 200) {
        var popper = self.poppers.pop();
    
        if (popper) {
          var task = res.body;
          popper.resolve({
            id: task.id,
            task: task
          });
        } else {
          // not sure what to do here... basically an abort
        }
        
        return self.backoff.reset();
      }
    } else if (err) {
      if (err.code !== 'ECONNREFUSED') {
        console.log(err.stack);
      }
    } else {
      console.log('really weird state... not sure this will ever happen...');
    }
    
    return self.backoff.next();
  });
};

HttpQueue.prototype.complete = function(messageID) {
  return q();
  // not sure what this means in this context
};

HttpQueue.prototype.abort = function(messageID) {
  return q();
  // not sure what this means in this context
};




var HttpQueueProvider = function(url) {
  if (!(this instanceof HttpQueueProvider)) {
    return new HttpQueueProvider(url);
  }
  
  if (typeof(url) === 'string') {
    this.url = {url: url};
  } else if (url && url.url) {
    this.url = url;
  }
  
  if (!this.url.method) { this.url.method = 'get'; }
  this.url.method = this.url.method.toLowerCase();
  
  if (['get', 'post', 'put'].indexOf(this.url.method) === -1) { throw new Error('HTTP Queue Provider does not work with ' + this.url.method + ' requests'); }
};

HttpQueueProvider.prototype.get = function(name, opts) {
  return new HttpQueue(this, name, opts);
};

module.exports = HttpQueueProvider;
