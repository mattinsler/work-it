-- KEYS:
--     1: <fair queue to push onto>
-- ARGV:
--     1: <task id to push>
--     2: <fairness key>

local len = redis.call('LPUSH', 'fq:' .. ARGV[2], ARGV[1])

if len == 1 then
  redis.call('LPUSH', 'f:' .. KEYS[1], ARGV[2])
end
