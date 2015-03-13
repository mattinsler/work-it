var q = require('q');
var AWS = require('aws-sdk');
var moment = require('moment');
var PassThrough = require('stream').PassThrough;

var S3Logger = function(provider, taskId, executionId) {
  this.taskId = taskId;
  this.executionId = executionId;
  this.endDeferred = q.defer();
  
  var s3 = new AWS.S3({
    accessKeyId: provider.opts.accessKeyId,
    secretAccessKey: provider.opts.secretAccessKey
  });
  
  var self = this;
  this.ended = false;
  this.bytesWritten = 0;
  this.stream = new PassThrough();
  
  // the upload doesn't actually start until you send some data
  this.upload = s3.upload({
    Bucket: provider.opts.bucket,
    Key: this.taskId + '/' + this.executionId + '.log',
    Body: this.stream
  }, function(err, data) {
    if (err) { return self.endDeferred.reject(err); }
    self.endDeferred.resolve(data.Location);
  });
};

S3Logger.prototype.log = function(line) {
  if (!this.ended) {
    var data = moment().format('YYYY-MM-DD[T]hh:mm:ss.SSSZZ') + ': ' + line.toString() + '\n';
    this.bytesWritten += Buffer.byteLength(data);
    this.stream.write(data);
  }
};

S3Logger.prototype.end = function() {
  this.ended = true;
  this.stream.end();
  
  if (this.bytesWritten === 0) {
    this.endDeferred.resolve();
  }
  
  return this.endDeferred.promise;
};



var S3LogProvider = function(opts) {
  if (!(this instanceof S3LogProvider)) {
    return new S3LogProvider(opts);
  }
  
  this.opts = opts || {};
  if (!this.opts.accessKeyId) { throw new Error('You must configure a S3LogProvider with accessKeyId'); }
  if (!this.opts.secretAccessKey) { throw new Error('You must configure a S3LogProvider with secretAccessKey'); }
  if (!this.opts.bucket) { throw new Error('You must configure a S3LogProvider with bucket'); }
};

S3LogProvider.prototype.get = function(taskId, executionId) {
  return new S3Logger(this, taskId, executionId);
};

module.exports = S3LogProvider;
