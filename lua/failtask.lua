-- KEYS:
--     1: <sorted set>
-- ARGV:
--     1: <item to fail>

-- remove working task
if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
  -- if removed, add to queue
  local machine, queue, failed, reaped, message = string.match(ARGV[1], "^([^|]*)|([^|]+)|([^|]+)|([^|]+)|(.*)$")
  redis.call('LPUSH', queue, tonumber(failed) + 1 .. "|" .. reaped .. "|" .. message)
end
