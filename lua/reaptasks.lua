-- KEYS:
--     1: <sorted set>
-- ARGV:
--     1: <maximum score of items to retry>

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local reapedIds = {}
local skippedIds = {}

-- ids are currently in working queue
-- if state of the task is currently working, then move to queued and remove id
-- if state of the task is currently queued, then remove just remove id

for idx, id in pairs(ids) do
  local data = redis.call('HMGET', 't:' .. id, 'a', 'r', 'rc')
  
  if data and data[1] and data[1] == 'w' and data[2] then
    -- set state to queued and increment reaped count
    redis.call('HMSET', 't:' .. id, 'a', 'q', 'rc', tonumber(data[3] or '0') + 1)
    redis.call('LPUSH', 'q:' .. data[2], id)
    reapedIds[#reapedIds + 1] = id
  else
    skippedIds[#skippedIds + 1] = id
  end
  
  redis.call('ZREM', KEYS[1], id)
end

return {reapedIds, skippedIds}
