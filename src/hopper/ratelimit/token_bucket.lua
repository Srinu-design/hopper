-- Token bucket: read, refill, take and write in one script, so it is atomic across every
-- API replica. GET-then-SET would let two replicas both read the last token and both allow;
-- MULTI/EXEC cannot branch on a value it read; WATCH retries pile up under contention.
--
-- KEYS[1]  bucket key, e.g. hopper:rl:<tenant id>:enqueue
-- ARGV[1]  capacity (the tenant's burst)
-- ARGV[2]  refill rate in tokens per second (the tenant's rate_per_sec), > 0
-- ARGV[3]  tokens requested
-- ARGV[4]  optional now in ms; only tests pass it. Otherwise Redis TIME is the one clock
--          for all replicas, so their clock skew never matters.
-- Returns { allowed (1 or 0), whole tokens left, ms until enough tokens (0 if allowed) }.

local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
if not now then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts = tonumber(state[2]) or now

tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate / 1000)

local allowed, retry_ms = 0, 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  retry_ms = math.ceil((requested - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
-- An idle bucket refills completely within capacity / rate; after twice that the key can go,
-- because a missing bucket starts full, which is the same thing.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) * 2)
return { allowed, math.floor(tokens), retry_ms }
