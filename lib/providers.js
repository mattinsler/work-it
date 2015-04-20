var providers = {
  Logger: {
    Console: require('./console-logger-provider'),
    S3: require('./s3-logger-provider')
  },
  Queue: {
    Http: require('./http-queue-provider'),
    Redis: require('./redis-queue-provider')
  },
  Storage: {
    Mongodb: require('./mongodb-storage-provider'),
    Mysql: require('./mysql-storage-provider')
  }
};

var providerMap = Object.keys(providers).reduce(function(o, s) {
  o[s.toLowerCase()] = Object.keys(providers[s]).reduce(function(oo, k) {
    oo[k.toLowerCase()] = providers[s][k];
    return oo;
  }, {});
  return o;
}, {});

providers.get = function(type, providerConfig, configuration) {
  if (providerConfig === undefined || providerConfig === null) {
    return null;
  }
  
  var providerType = providerMap[type.toLowerCase()];
  if (providerType) {
    var subtype;
    var subtypeConfig;
    if (typeof(providerConfig) === 'string') {
      subtype = providerConfig;
    } else {
      subtype = Object.keys(providerConfig)[0];
      subtypeConfig = providerConfig[subtype];
    }
  
    if (providerType[subtype]) {
      return providerType[subtype](configuration, subtypeConfig);
    }
  }
  
  return null;
};

module.exports = providers;
