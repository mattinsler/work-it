-- KEYS:
--     1: <sorted set to remove from>
-- ARGV:
--     1: <id of task to remove>

redis.call('DEL', 't:' .. ARGV[1])
return redis.call('ZREM', KEYS[1], ARGV[1])
