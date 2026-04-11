local key = KEYS[1]
local windowSeconds = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

if not windowSeconds or not limit or not now then
  return redis.error_reply("Invalid input parameters")
end

local windowMillis = windowSeconds * 1000

redis.call('ZREMRANGEBYSCORE', key, 0, now - windowMillis)

local current = redis.call('ZCARD', key)

if current < limit then
  math.randomseed(now)
  local random = math.random(1000000)
  redis.call('ZADD', key, now, now .. '-' .. random)
  redis.call('EXPIRE', key, windowSeconds)
  return current + 1
end

return 0
