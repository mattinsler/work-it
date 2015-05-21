-- KEYS:
--     1: <the fair queue>
-- ARGV:
--     1: <the fair key to clear>


local ids = redis.call('LRANGE', 'fq:' .. ARGV[1], 0, -1)
for x = 1, #ids do
  ids[x] = 't:' .. ids[x]
end

redis.call('LREM', KEYS[1], 0, ARGV[1])
redis.call('DEL', 'fq:' .. ARGV[1])
return redis.call('DEL', unpack(ids))
