package com.example.order.inventory.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.order.common.BusinessException;
import com.example.order.common.ResultCode;
import com.example.order.inventory.entity.Inventory;
import com.example.order.inventory.mapper.InventoryMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final InventoryMapper inventoryMapper;

    /**
     * Query inventory by productId
     */
    public Inventory getByProductId(Long productId) {
        return inventoryMapper.selectOne(
                new LambdaQueryWrapper<Inventory>()
                        .eq(Inventory::getProductId, productId));
    }

    /**
     * Deduct stock: available_stock - quantity, locked_stock + quantity
     */
    public void deductStock(Long productId, int quantity) {
        int rows = inventoryMapper.deductStock(productId, quantity);
        if (rows == 0) {
            throw new BusinessException(ResultCode.STOCK_NOT_ENOUGH);
        }
        log.info("Stock deducted: productId={}, quantity={}", productId, quantity);
    }

    /**
     * Rollback stock: locked_stock - quantity, available_stock + quantity
     */
    public void rollbackStock(Long productId, int quantity) {
        int rows = inventoryMapper.rollbackStock(productId, quantity);
        if (rows == 0) {
            log.warn("Rollback stock failed: productId={}, quantity={}", productId, quantity);
        }
        log.info("Stock rolled back: productId={}, quantity={}", productId, quantity);
    }

    /**
     * Confirm stock: locked_stock - quantity, total_stock - quantity
     */
    public void confirmStock(Long productId, int quantity) {
        int rows = inventoryMapper.confirmStock(productId, quantity);
        if (rows == 0) {
            log.warn("Confirm stock failed: productId={}, quantity={}", productId, quantity);
        }
        log.info("Stock confirmed: productId={}, quantity={}", productId, quantity);
    }
}