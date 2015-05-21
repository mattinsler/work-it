-- fair version

-- KEYS:
--     1: <fair queue to pop from>
--     2: <sorted set to add to>
-- ARGV:
--     1: <score to use in sorted set>

local function pop ()
  local fairKey = redis.call('RPOP', 'f:' .. KEYS[1])

  if fairKey == false then
    return nil
  else
    local id = redis.call('RPOP', 'fq:' .. fairKey)

    if id == false then
      return false
    else
      -- if queue had an item, then push fair key back onto fair queue
      redis.call('LPUSH', 'f:' .. KEYS[1], fairKey)

      redis.call('ZADD', KEYS[2], ARGV[1], id)
      -- set state to working
      redis.call('HSET', 't:' .. id, 'a', 'w')
      local item = redis.call('HGETALL', 't:' .. id)
      item[#item + 1] = 'id'
      item[#item + 1] = id
      return item
    end
  end
end


local item
repeat
  item = pop()
until item ~= false

return item
