-- KEYS:
--     1: <sorted set to add to>
-- ARGV:
--     1: <id of task to adopt>
--     2: <score to use in sorted set>

redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
-- set state to working
redis.call('HSET', 't:' .. ARGV[1], 'a', 'w')
local item = redis.call('HGETALL', 't:' .. ARGV[1])
item[#item + 1] = 'id'
item[#item + 1] = ARGV[1]
return item
