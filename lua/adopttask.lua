-- KEYS:
--     1: <sorted set to add to>
-- ARGV:
--     1: <id of task to adopt>
--     2: <score to use in sorted set>

local id = ARGV[1]

if id == false then
  return nil
else
  redis.call('ZADD', KEYS[1], ARGV[2], id)
  -- set state to working
  redis.call('HSET', 't:' .. id, 'a', 'w')
  local item = redis.call('HGETALL', 't:' .. id)
  item[#item + 1] = 'id'
  item[#item + 1] = id
  return item
end
