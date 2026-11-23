-- Closed three-host fixture. Each ingress attests to the activation its read reached.
-- Compare against the writer observed before routing, so a delayed read cannot
-- cast a vote in a later handoff. Retired activation IDs can never regain the grant.
local writer = redis.call('GET', KEYS[1]) or ''
local expected, voter, candidate = ARGV[1], ARGV[2], ARGV[3]
if voter ~= 'n0' and voter ~= 'n1' and voter ~= 'n2' then return 0 end
if writer ~= expected then return 0 end
if writer == candidate or redis.call('SISMEMBER', KEYS[3], candidate) == 1 then
    redis.call('HDEL', KEYS[2], voter)
    return 0
end
redis.call('HSET', KEYS[2], voter, candidate)
local count = 0
for _, value in ipairs(redis.call('HVALS', KEYS[2])) do
    if value == candidate then count = count + 1 end
end
if count < 2 then return 0 end
if writer ~= '' then redis.call('SADD', KEYS[3], writer) end
redis.call('SET', KEYS[1], candidate)
redis.call('DEL', KEYS[2])
return 1
