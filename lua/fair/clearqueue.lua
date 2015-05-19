-- KEYS:
--     1: <the queue to clear>

-- loop through all fair keys and clear all queues underneath. then remove fair queue

local fairKeys = redis.call('LRANGE', KEYS[1], 0, -1)

for x = 1, #fairKeys do
  fairKeys[x] = 'q:' .. fairKeys[x]

  local ids = redis.call('LRANGE', fairKeys[x], 0, -1)
  for y = 1, #ids do
    ids[y] = 't:' .. ids[y]
  end

  redis.call('DEL', fairKeys[x])
  redis.call('DEL', unpack(ids))
end

return redis.call('DEL', KEYS[1])
