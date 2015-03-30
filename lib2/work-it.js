var fs = require('fs');
var path = require('path');
var Configuration = require('./configuration');

exports.version = require('../package.json').version;

exports.configure = function(config) {
  return new Configuration(config);
};

exports.configureFromFile = function(filename) {
  if (!fs.existsSync(filename)) { throw new Error('Could not file configuration file at ' + filename); }
  if (path.extname(filename) !== '.json') { throw new Error('Configuration must be a JSON file'); }
  
  var config;
  try {
    config = require(filename);
  } catch (err) {
    throw new Error('Could not parse configuration file at ' + filename + ': ' + err.message);
  }
  
  return exports.configure(config);
};
