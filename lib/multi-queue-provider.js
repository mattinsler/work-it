var MultiQueueProvider = function(providerMap) {
  if (!(this instanceof MultiQueueProvider)) {
    return new MultiQueueProvider(providerMap);
  }
  
  this.providerMap = providerMap;
};

MultiQueueProvider.prototype.get = function(name, opts) {
  if (this.providerMap[name]) {
    return this.providerMap[name].get(name, opts);
  } else if (this.providerMap['*']) {
    return this.providerMap['*'].get(name, opts);
  } else if (this.providerMap.default) {
    return this.providerMap.default.get(name, opts);
  } else {
    throw new Error('MultiQueueProvider cannot find a queue provider for ' + name + '. You should specify a "default" or "*" queue provider.');
  }
};

module.exports = MultiQueueProvider;
