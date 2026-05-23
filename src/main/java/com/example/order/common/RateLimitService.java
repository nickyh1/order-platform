package com.example.order.common;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RateLimitService {

    private final StringRedisTemplate redisTemplate;

    /**
     * Sliding window rate limiter using Redis.
     * @param key     rate limit key (e.g. "rate_limit:order:userId:1")
     * @param limit   max requests allowed
     * @param windowSeconds  time window in seconds
     * @return true if request is allowed, false if rate limited
     */
    public boolean isAllowed(String key, int limit, int windowSeconds) {
        long now = System.currentTimeMillis();
        long windowStart = now - windowSeconds * 1000L;

        // Remove entries outside the window
        redisTemplate.opsForZSet().removeRangeByScore(key, 0, windowStart);

        // Count entries in current window
        Long count = redisTemplate.opsForZSet().zCard(key);
        if (count != null && count >= limit) {
            log.warn("Rate limited: key={}, count={}, limit={}", key, count, limit);
            return false;
        }

        // Add current request
        redisTemplate.opsForZSet().add(key, String.valueOf(now), now);
        redisTemplate.expire(key, windowSeconds, TimeUnit.SECONDS);

        return true;
    }
}