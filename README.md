# work-it

Worker system with pluggable components

## Objects

### QueueProvider

#### QueueProvider::get(commandName)

Returns a Queue

### Queue

#### Queue::push()

#### Queue::pop()

Returns a promise that resolves to a popped task.

Tasks look like:

```javascript
{
  id: '',
  data: {
    // ...
  }
}
```

#### Queue::delete(messageID)

