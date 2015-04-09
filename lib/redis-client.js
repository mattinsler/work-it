var q = require('q');
var fs = require('fs');
var redis = require('redis');
var redisBuilder = require('redis-builder')(redis);

var RedisClient = function(url, opts, scripts) {
  if (!(this instanceof RedisClient)) {
    return new RedisClient(url, opts, scripts);
  }
  
  this.url = url;
  this.opts = opts || {};
  this.scriptSpecs = scripts || {};
  
  var self = this;
  this.scripts = Object.keys(scripts).reduce(function(o, k) {
    o[k] = function() {
      var args = Array.prototype.slice.call(arguments);
      return self._connect().then(function() {
        return self.scripts[k].apply(self, args);
      });
    };
    
    return o;
  }, {});
  
  this.multiScripts = {};
};

RedisClient.prototype._createScript = function(client, commandName, spec) {
  var lua;
  if (spec.lua) {
    lua = spec.lua;
  } else if (spec.luafile) {
    lua = fs.readFileSync(spec.luafile).toString();
  } else {
    throw new Error('Scripts must have either a `lua` or `luafile` key');
  }
  
  var numKeys = spec.keys || 0;
  var numArgs = spec.args || 0;
  
  var d = q.defer();
  var self = this;
  
  client.script('load', lua, function(err, sha) {
    if (err) { return d.reject(err); }
    
    self.scripts[commandName] = function() {
      var callback;
      var args = Array.prototype.slice.call(arguments);
      
      if (args.length !== numKeys + numArgs) {
        throw new Error('Cannot call script ' + commandName + ': Expected ' + (numKeys + numArgs) + ' arguments but only received ' + args.length);
      }
      return self.evalsha.apply(self, [sha, numKeys].concat(args.slice(0, numKeys), args.slice(numKeys)));
    };
    
    self.multiScripts[commandName] = function(multi) {
      var args = Array.prototype.slice.call(arguments, 1);
      
      if (args.length !== numKeys + numArgs) {
        throw new Error('Cannot add script ' + commandName + ' to multi: Expected ' + (numKeys + numArgs) + ' arguments but only received ' + args.length);
      }
      return multi.evalsha.apply(multi, [sha, numKeys].concat(args.slice(0, numKeys), args.slice(numKeys)));
    };
    
    d.resolve();
  });
  
  return d.promise;
};

RedisClient.prototype._connect = function() {
  if (this._clientDeferred) { return this._clientDeferred.promise; }
    
  var self = this;
  this._clientDeferred = q.defer();
  
  var client = redisBuilder(this.url, this.opts);
  
  client.on('ready', function() {
    if (!self.scriptSpecs || Object.keys(self.scriptSpecs).length === 0) {
      self._clientDeferred.resolve(client);
    }
    
    q.all(
      Object.keys(self.scriptSpecs).map(function(commandName) {
        return self._createScript(client, commandName, self.scriptSpecs[commandName]);
      })
    ).then(function() {
      self._clientDeferred.resolve(client);
    });
  });
  
  client.on('error', function(err) {
    self._clientDeferred.reject(err);
  });
  
  return this._clientDeferred.promise;
};

Object.keys(redis.RedisClient.prototype).filter(function(k) {
  return k.toUpperCase() === k && typeof(redis.RedisClient.prototype[k]) === 'function';
}).filter(function(k) {
  return k !== 'MULTI';
}).forEach(function(commandName) {
  RedisClient.prototype[commandName] = function() {
    var args = Array.prototype.slice.call(arguments);
    return this._connect().then(function(client) {
      return q.ninvoke.apply(q, [client, commandName].concat(args));
    });
  };
  
  RedisClient.prototype[commandName.toLowerCase()] = function() {
    var args = Array.prototype.slice.call(arguments);
    return this._connect().then(function(client) {
      return q.ninvoke.apply(q, [client, commandName].concat(args));
    });
  };
});

module.exports = RedisClient;
