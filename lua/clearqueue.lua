-- KEYS:
--     1: <the queue to clear>

local ids = redis.call('LRANGE', KEYS[1], 0, -1)

for x = 1, #ids do
  redis.call('DEL', 't:' .. ids[x])
end

return redis.call('DEL', KEYS[1])
