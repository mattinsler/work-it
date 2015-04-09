-- KEYS:
--     1: <sorted set>
-- ARGV:
--     1: <task ID to fail>

-- remove working task
if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
  -- if removed
  
  local data = redis.call('HMGET', 't:' .. ARGV[1], 'fc', 'r')
  redis.call('HMSET', 't:' .. ARGV[1], 'fc', tonumber(data[1]) + 1, 'a', 'q')
  
  return redis.call('LPUSH', 'q:' .. data[2], ARGV[1])
end
