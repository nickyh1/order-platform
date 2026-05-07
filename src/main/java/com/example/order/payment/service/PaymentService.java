package com.example.order.payment.service;

import com.example.order.common.BusinessException;
import com.example.order.common.ResultCode;
import com.example.order.inventory.service.InventoryService;
import com.example.order.order.entity.OrderInfo;
import com.example.order.order.entity.OrderStatus;
import com.example.order.order.mapper.OrderMapper;
import com.example.order.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final OrderService orderService;
    private final OrderMapper orderMapper;
    private final InventoryService inventoryService;

    /**
     * Handle payment callback.
     * Success: PENDING -> PAID, locked_stock -> sold (total_stock decreases)
     * Failure: PENDING -> CANCELLED, rollback stock
     */
    @Transactional(rollbackFor = Exception.class)
    public OrderInfo handleCallback(String orderNo, boolean success) {
        OrderInfo order = orderService.getByOrderNo(orderNo);
        OrderStatus currentStatus = OrderStatus.fromValue(order.getStatus());

        // Idempotent: if already in terminal state, return directly
        if (currentStatus != OrderStatus.PENDING) {
            log.info("Order {} already in terminal state: {}, skip callback", orderNo, currentStatus);
            return order;
        }

        if (success) {
            // Payment success
            OrderStatus target = OrderStatus.PAID;
            if (!currentStatus.canTransitTo(target)) {
                throw new BusinessException(ResultCode.ORDER_STATUS_INVALID);
            }

            order.setStatus(target.getValue());
            order.setPaymentTime(LocalDateTime.now());
            orderMapper.updateById(order);

            // locked_stock -> sold
            inventoryService.confirmStock(order.getProductId(), order.getQuantity());
            log.info("Payment success: orderNo={}", orderNo);

        } else {
            // Payment failed
            OrderStatus target = OrderStatus.CANCELLED;
            if (!currentStatus.canTransitTo(target)) {
                throw new BusinessException(ResultCode.ORDER_STATUS_INVALID);
            }

            order.setStatus(target.getValue());
            orderMapper.updateById(order);

            // Rollback stock
            inventoryService.rollbackStock(order.getProductId(), order.getQuantity());
            log.info("Payment failed, order cancelled: orderNo={}", orderNo);
        }

        return order;
    }
}