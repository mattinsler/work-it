-- KEYS:
--     1: <sorted set>
-- ARGV:
--     1: <maximum score of items to retry>

local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])

for idx, item in pairs(items) do
  local machine, queue, failed, reaped, message = string.match(item, "^([^|]*)|([^|]+)|([^|]+)|([^|]+)|(.*)$")
  redis.call('LPUSH', queue, failed .. "|" .. tonumber(reaped) + 1 .. "|" .. message)
  redis.call('ZREM', KEYS[1], item)
end

return items
