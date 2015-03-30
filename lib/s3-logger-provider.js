var S3Logger = require('./s3-logger');

module.exports = function(configuration, config) {
  if (!config) { config = {}; }
  if (!config.accessKeyId) { throw new Error('You must configure a S3LoggerProvider with accessKeyId'); }
  if (!config.secretAccessKey) { throw new Error('You must configure a S3LoggerProvider with secretAccessKey'); }
  if (!config.bucket) { throw new Error('You must configure a S3LoggerProvider with bucket'); }
  
  var creds = {
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey
  };
  var bucket = config.bucket;
  
  return {
    get: function(task, execution) {
      return new S3Logger(creds, bucket, task, execution);
    }
  };
};
