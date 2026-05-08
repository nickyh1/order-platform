package com.example.order.inventory.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.order.inventory.entity.Inventory;
import com.example.order.inventory.mapper.InventoryMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final InventoryMapper inventoryMapper;
    private final Map<String, StockDeductStrategy> strategyMap;
    private final DbOptimisticLockStrategy dbStrategy;
    private final RedisPreDeductStrategy redisStrategy;

    /**
     * Switch strategy via application.yml:
     * stock.strategy=db  or  stock.strategy=redis
     */
    @Value("${stock.strategy:db}")
    private String strategyType;

    private StockDeductStrategy getStrategy() {
        return "redis".equalsIgnoreCase(strategyType) ? redisStrategy : dbStrategy;
    }

    public Inventory getByProductId(Long productId) {
        return inventoryMapper.selectOne(
                new LambdaQueryWrapper<Inventory>()
                        .eq(Inventory::getProductId, productId));
    }

    public void deductStock(Long productId, int quantity) {
        getStrategy().deduct(productId, quantity);
    }

    public void rollbackStock(Long productId, int quantity) {
        getStrategy().rollback(productId, quantity);
    }

    public void confirmStock(Long productId, int quantity) {
        getStrategy().confirm(productId, quantity);
    }

    public String currentStrategy() {
        return getStrategy().name();
    }
}