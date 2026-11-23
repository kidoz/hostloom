-- One entity per fixture. Fence before deduplicating or changing durable state.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 'fenced'
end
if redis.call('SADD', KEYS[2], ARGV[2]) == 0 then
    return 'duplicate'
end
redis.call('RPUSH', KEYS[3], ARGV[2] .. '@' .. ARGV[1])
return 'committed'
