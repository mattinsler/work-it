-- KEYS:
--     1: <queue to pop from>
--     2: <sorted set to add to>
-- ARGV:
--     1: <score to use in sorted set>
--     2: <string to prepend to item>

local item = redis.call('RPOP', KEYS[1])

if item == false then
  return nil
else
  item = ARGV[2] .. item
  redis.call('ZADD', KEYS[2], ARGV[1], item)
  return item
end
