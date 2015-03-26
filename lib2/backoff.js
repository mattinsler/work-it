var Progression = {
  fibonacci: function() {
    var last = 1;
    return function(current) {
      last = current + last;
      return last;
    }
  }
};


var Backoff = function(fn, scope) {
  this.fn = fn.bind(scope);
  
  this.progression = Progression.fibonacci();
  this.limits = {
    min: 0,
    max: 2500
  };
  this.current = this.limits.min;
};

Backoff.prototype.reset = function() {
  if (this.timeoutId) { clearTimeout(this.timeoutId); }
  
  this.current = this.limits.min;
  this.timeoutId = setTimeout(this.fn, this.current);
};

Backoff.prototype.next = function() {
  if (this.timeoutId) { clearTimeout(this.timeoutId); }
  
  this.current = Math.min(this.progression(this.current), this.limits.max);
  this.timeoutId = setTimeout(this.fn, this.current);
};

Backoff.prototype.immediate = function() {
  setImmediate(this.fn);
};

Backoff.prototype.stop = function() {
  if (this.timeoutId) {
    clearTimeout(this.timeoutId);
    delete this.timeoutId;
  }
};

module.exports = Backoff;
