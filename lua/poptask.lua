-- KEYS:
--     1: <queue to pop from>
--     2: <sorted set to add to>
-- ARGV:
--     1: <score to use in sorted set>

local id = redis.call('RPOP', KEYS[1])

if id == false then
  return nil
else
  redis.call('ZADD', KEYS[2], ARGV[1], id)
  -- set state to working
  redis.call('HSET', 't:' .. id, 'a', 'w')
  local item = redis.call('HGETALL', 't:' .. id)
  item[#item + 1] = 'id'
  item[#item + 1] = id
  return item
end
