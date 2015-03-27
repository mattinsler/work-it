var q = require('q');
var AWS = require('aws-sdk');
var moment = require('moment');
var PassThrough = require('stream').PassThrough;

var S3Logger = function(creds, bucket, task, execution) {
  this.task = task;
  this.execution = execution;
  this.endDeferred = q.defer();
  
  var s3 = new AWS.S3(creds);
  
  var self = this;
  this.ended = false;
  this.bytesWritten = 0;
  this.stream = new PassThrough();
  
  // the upload doesn't actually start until you send some data
  this.upload = s3.upload({
    Bucket: bucket,
    Key: task + '/' + execution + '.log',
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

module.exports = S3Logger;
