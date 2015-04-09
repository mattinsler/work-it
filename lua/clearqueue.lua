-- KEYS:
--     1: <the queue to clear>

local ids = redis.call('LRANGE', KEYS[1], 0, -1)

for x = 1, #ids do
  ids[x] = 't:' .. ids[x]
end

redis.call('DEL', KEYS[1])
return redis.call('DEL', unpack(ids))
