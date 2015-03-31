# work-it

Worker system with pluggable components

## Usage

#### Configuration

```javascript
var workit = require('work-it');

var configured = workit.configure({
  redis: 'redis://localhost/12',
  queues: {
    '*': 'redis'
  },
  loggers: [
    'console',
    {console: ''}       // optional way to specify a logger with no config
    {
      s3: {
        accessKeyId: '...',
        secretAccessKey: '...',
        bucket: 'task-logs'
      }
    }
  ],
  storage: {
    mongodb: 'mongodb://localhost/work'
  }
});

// or 
var configured = workit.configureFromFile('workit.json');

// create a configured worker
var worker = configured.worker('send-email', require.resolve('./send-email.js'), {
  concurrency: 4     // optional, default is 1
});

// create a task manager
var taskManager = configured.taskManager();
```

#### Worker

```javascript
var worker = ...;

worker.start();

process.on('SIGINT', function() {
  worker.stop().then(function() {
    process.exit();
  });
});
```

#### TaskManager

```javascript
var taskManager = ...;

// queue a new task
taskManager.queueTask('send-email', {
  to: 'matt.insler@gmail.com',
  subject: 'Hello',
  body: "What's up!"
}).then(function(taskTracker) {
  // You can get the task ID from the tracker
  console.log(taskTracker.id);
});

// optionally you can add tags that can be searched for later
taskManager.queueTask('test', {
  foo: 'bar'
}, {
  tags: ['hello', 'world']
});

/*
  There are some cases where you'd like to have a pull "queue" rather
  than a push queue. In this case, you'll need to tell the TaskManager
  about the task when it gets pulled. You can call this method and return
  the value it creates to the requesting client.
*/
taskManager.startTaskWork('send-email', {
  to: 'matt.insler@gmail.com',
  subject: 'Hello Pull Queue',
  body: "What's up!"
}, {
  popper: popperId,         // string
  retry: retryQueueName,    // string
  tags: ['hello', 'world']  // optional list of strings
}).then(function(taskSlug) {
  // send `taskSlug` to the requesting client
});

// or track a task by ID
var taskTracker = taskManager.taskTracker('some-task-id');

// you can track the start and finish events
taskTracker.on('start', function(data) {
  console.log('START', data);
});

taskTracker.on('finish', function(data) {
  console.log('FINISH', data);
});

// tasks currently working
taskManager.workingTasks().then(console.log);
// tasks older than 5 seconds ago
taskManager.workingTasksOlderThan(5000).then(console.log);
```
