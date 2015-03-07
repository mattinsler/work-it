var encoder = require('./encoder');
var redisBuilder = require('redis-builder');
var EventEmitter = require('events').EventEmitter;

var VALID_EVENTS = ['start', 'finish'];

var RedisTaskStatusEvents = function(task, provider) {
  this.task = task;
  this.topic = 't:' + task;
  
  this.subscriber = provider.subscriber;
  this.publisher = provider.publisher;
  
  this.emitter = new EventEmitter();
  this.onMessage = function(channel, message) {
    if (channel !== this.topic) { return; }
    var data = encoder.decode(message);
    this.emitter.emit(data.e, data.d);
  }.bind(this);
};

RedisTaskStatusEvents.prototype.on = function(event, callback) {
  if (VALID_EVENTS.indexOf(event) === -1) { return; }
  
  if (Object.keys(this.emitter._events).length === 0) {
    this.subscriber.subscribe(this.topic);
    this.subscriber.on('message', this.onMessage);
  }
  
  this.emitter.on(event, callback);
};

RedisTaskStatusEvents.prototype.removeListener = function(event, callback) {
  this.emitter.removeListener(event, callback);
  
  if (Object.keys(this.emitter._events).length === 0) {
    this.subscriber.removeListener('message', this.onMessage);
    this.subscriber.unsubscribe(this.topic);
  }
};

RedisTaskStatusEvents.prototype._publish = function(message) {
  return this.publisher.publish(this.topic, encoder.encode(message));
};

RedisTaskStatusEvents.prototype.start = function(data) {
  return this._publish({
    e: 'start',
    d: data
  });
};

RedisTaskStatusEvents.prototype.finish = function(data) {
  return this._publish({
    e: 'finish',
    d: data
  });
};




var RedisTaskStatusEventsProvider = function(url) {
  if (!(this instanceof RedisTaskStatusEventsProvider)) {
    return new RedisTaskStatusEventsProvider(url);
  }
  
  this.url = url;
  this.subscriber = redisBuilder({url: url});
  this.publisher = redisBuilder({url: url});
};

RedisTaskStatusEventsProvider.prototype.get = function(task) {
  return new RedisTaskStatusEvents(task, this);
};

module.exports = RedisTaskStatusEventsProvider;
