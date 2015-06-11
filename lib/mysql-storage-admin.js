var q = require('q');
var uuid = require('node-uuid');

var isDefined = function(v) {
  return typeof(v) !== 'undefined' && v !== null;
};

var TaskQueryBuilder = function(connection) {
  this.opts = {};
  this.connection = connection;

  this.rowConverter = function(row) {
    row.task_id = uuid.unparse(row.task_id);
    row.tags = (row.tags || '').split(',');
    row.data = JSON.parse(row.data);
    row.queued_at = new Date(row.queued_at);
    row.started_at = new Date(row.started_at);
    row.finished_at = new Date(row.finished_at);

    row.success = row.success === 1;

    return row;
  };
};

TaskQueryBuilder.prototype.limit = function(limit) {
  this.opts.limit = limit;
  return this;
};

TaskQueryBuilder.prototype.orderBy = function(orderBy) {
  this.opts.orderBy = orderBy;
  return this;
};

/** ...where('foo = ? AND bar = ?', 'hello', 'world') **/
TaskQueryBuilder.prototype.where = function(query) {
  var args = Array.prototype.slice.call(arguments, 1);
  if (!this.opts.where) { this.opts.where = {query: [], args: []}; }
  this.opts.where.query.push(query);
  this.opts.where.args.push(args);
  return this;
};

TaskQueryBuilder.prototype.select = function() {
  var d = q.defer();

  var args = [];
  var query = ['SELECT GROUP_CONCAT(tt.id) as tags, te.* FROM task_executions te LEFT JOIN task_tags tt ON (tt.task_id = te.task_id)'];

  if (this.opts.where && this.opts.where.query.length > 0) {
    query.push('WHERE ' + this.opts.where.query.join(' AND '));
    Array.prototype.push.apply(args, this.opts.where.args);
  }

  query.push('GROUP BY te.task_id');

  if (isDefined(this.opts.limit)) {
    query.push('LIMIT ' + this.opts.limit);
  }
  if (isDefined(this.opts.orderBy)) {
    var orderBy = this.opts.orderBy;
    var orderStr = Object.keys(orderBy).map(function(k) {
      return k + ' ' + (orderBy[k] > 0 ? 'ASC' : 'DESC');
    }).join(', ');

    if (orderStr) { query.push('ORDER BY ' + orderStr); }
  }

  var rowConverter = this.rowConverter;
  this.connection.query.apply(this.connection, [query.join(' ')].concat(args).concat(function(err, rows) {
    if (err) { return d.reject(err); }
    if (rowConverter) { rows = rows.map(rowConverter); }
    d.resolve(rows);
  }));

  return d.promise;
};


var MysqlStorageAdmin = function(connection) {
  this.connection = connection;
};

MysqlStorageAdmin.prototype._taskQuery = function() {
  return new TaskQueryBuilder(this.connection);
}

MysqlStorageAdmin.prototype._query = function() {
  var d = q.defer();
  var query = Array.prototype.slice.call(arguments);

  this.connection.query.apply(this.connection, query.concat(function(err, rows) {
    if (err) { return d.reject(err); }
    d.resolve(rows);
  }));

  return d.promise;
};

MysqlStorageAdmin.prototype.listTasks = function(opts) {
  if (!opts) { opts = {}; }
  return this._taskQuery().orderBy({started_at: -1}).limit(opts.limit || 100).select();
};

MysqlStorageAdmin.prototype.listTasksByTag = function(tag) {
  // return this._query('SELECT * from task_executions WHERE task_id IN (SELECT task_id FROM task_tags WHERE id = ?) ORDER BY started_at DESC', tag).then(ConvertTask);
  return this._taskQuery().where('te.task_id IN (SELECT task_id FROM task_tags WHERE id = ?)', tag).orderBy({started_at: -1}).select();
};

MysqlStorageAdmin.prototype.listTasksLikeTag = function(tag) {
  return this._taskQuery().where('te.task_id IN (SELECT task_id FROM task_tags WHERE id LIKE ?)', tag).orderBy({started_at: -1}).select();
};

module.exports = MysqlStorageAdmin;
