-- KEYS:
--     1: <sorted set>
-- ARGV:
--     1: <maximum score of items to retry>

local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])

for idx, item in pairs(items) do
  local machine, queue, message = string.match(item, "^([^|]*)|([^|]+)|([^|]+)$")
  redis.call('LPUSH', queue, message)
  redis.call('ZREM', KEYS[1], item)
end

return items
