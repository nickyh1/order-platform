package com.example.order.product.service;

import com.example.order.product.entity.Product;
import com.example.order.product.mapper.ProductMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.util.concurrent.TimeUnit;

@Slf4j
@Mapper
@Service
@RequiredArgsConstructor
public class ProductCacheService {

    private final StringRedisTemplate redisTemplate;
    private final ProductMapper productMapper;
    private final ObjectMapper objectMapper;

    private static final String CACHE_KEY_PREFIX = "product:detail:";
    private static final long CACHE_TTL_MINUTES = 30;
    private final com.example.order.mq.OrderMessageProducer messageProducer;

    /**
     * Evict cache with MQ retry as fallback.
     * If Redis delete fails, save a message to outbox table for retry.
     */
    public void evictCacheReliable(Long productId) {
        try {
            evictCache(productId);
        } catch (Exception e) {
            log.error("Failed to evict cache, sending retry message: productId={}", productId, e);
            // Use message table as fallback, retry task will pick it up
            messageProducer.saveMessageLog(
                    productId,
                    "CACHE_EVICT",
                    java.util.Map.of("productId", productId)
            );
        }
    }

    /**
     * Delayed double delete: delete now, then delete again after delay.
     * Handles the race condition where a read thread caches stale data
     * between our DB update and cache delete.
     */
    public void evictCacheWithDelay(Long productId) {
        // First delete
        evictCache(productId);

        // Schedule second delete after 500ms
        new Thread(() -> {
            try {
                Thread.sleep(500);
                evictCache(productId);
                log.info("Delayed double delete completed: productId={}", productId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Delayed delete interrupted: productId={}", productId);
            }
        }).start();
    }

    /**
     * Get product from cache first, fallback to DB.
     */
    public Product getProductWithCache(Long productId) {
        String key = CACHE_KEY_PREFIX + productId;

        // 1. Try cache
        String cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            log.debug("Cache hit: productId={}", productId);
            try {
                return objectMapper.readValue(cached, Product.class);
            } catch (Exception e) {
                log.warn("Failed to deserialize cached product, fallback to DB", e);
                redisTemplate.delete(key);
            }
        }

        // 2. Cache miss, query DB
        log.debug("Cache miss: productId={}", productId);
        Product product = productMapper.selectById(productId);
        if (product == null) {
            return null;
        }

        // 3. Write to cache
        try {
            String json = objectMapper.writeValueAsString(product);
            redisTemplate.opsForValue().set(key, json, CACHE_TTL_MINUTES, TimeUnit.MINUTES);
            log.info("Product cached: productId={}", productId);
        } catch (Exception e) {
            log.warn("Failed to cache product: productId={}", productId, e);
        }

        return product;
    }

    /**
     * Delete cache (used when product is updated).
     */
    public void evictCache(Long productId) {
        String key = CACHE_KEY_PREFIX + productId;
        redisTemplate.delete(key);
        log.info("Product cache evicted: productId={}", productId);
    }
}