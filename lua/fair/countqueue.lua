-- KEYS:
--     1: <the fair queue to count>

-- loop through all fair keys and all queues underneath

local fairKeys = redis.call('LRANGE', KEYS[1], 0, -1)
local total = 0

for x = 1, #fairKeys do
  total = total + redis.call('LLEN', 'q:' .. fairKeys[x])
end

return total
