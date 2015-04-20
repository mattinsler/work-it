var q = require('q');
var crypto = require('crypto');
var uuid = require('node-uuid');

var MysqlStorage = function(connection, opts) {
  this.connection = connection;
  this.task = opts.task;
  this.execution = opts.execution;
  
  this.task_id = Buffer(uuid.parse(this.task));
};

MysqlStorage.prototype._transaction = function(queries) {
  if (!Array.isArray(queries)) { queries = [queries]; }
  
  var d = q.defer();
  var sql = [];
  var connection = this.connection;
  
  connection.beginTransaction(function(err) {
    if (err) {
      return d.reject(err);
    }
    
    var next = function(idx) {
      if (idx === queries.length) {
        return connection.commit(function(err) {
          if (err) {
            return connection.rollback(function() {
              d.reject(err);
            });
          }
          d.resolve(sql);
        });
      }
      
      var query = queries[idx];
      if (!Array.isArray(query)) { query = [query]; }
      
      sql.push(connection.query.apply(connection, query.concat(function(err, result) {
        if (err) {
          return connection.rollback(function() {
            d.reject(err);
          });
        }
        
        next(idx + 1);
      })).sql);
    };
    
    next(0);
  });
  
  return d.promise;
};

MysqlStorage.prototype.start = function(update) {
  var queries = [
    [
      'INSERT IGNORE INTO tasks SET ?', {
        id: this.task_id,
        data: typeof(update.data) === 'string' ? update.data : JSON.stringify(update.data),
        queue: update.queue,
        retry_queue: update.retryQueue
      }
    ], [
      'INSERT INTO executions SET ?', {
        task_id: this.task_id,
        execution_id: this.execution,
        queued_at: update.queuedAt.getTime(),
        started_at: update.startedAt.getTime(),
        failed_count: update.failedCount,
        reaped_count: update.reapedCount
      }
    ]
  ];
  
  for (var x = 0; x < update.tags.length; ++x) {
    queries.push([
      'INSERT IGNORE INTO task_tags SET ?', {
        id: update.tags[x],
        task_id: this.task_id
      }
    ]);
  }
  
  return this._transaction(queries);
};

MysqlStorage.prototype.finish = function(update) {
  var errorId = null;
  var queries = [];
  
  if (update.error) {
    errorId = crypto.createHash('sha256').update(update.error).digest();
    queries.push([
      'INSERT IGNORE INTO execution_errors SET ?', {
        id: errorId,
        error: update.error
      }
    ]);
  }
  
  queries.push([
    'UPDATE executions SET ? WHERE task_id = ? AND execution_id = ?', [
      {
        finished_at: update.finishedAt.getTime(),
        success: update.success,
        error_id: errorId
      },
      this.task_id,
      this.execution
    ]
  ]);
  
  return this._transaction(queries);
};

module.exports = MysqlStorage;
