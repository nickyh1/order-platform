package com.example.order.order.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.order.common.BusinessException;
import com.example.order.common.ResultCode;
import com.example.order.inventory.service.InventoryService;
import com.example.order.order.entity.CreateOrderRequest;
import com.example.order.order.entity.OrderInfo;
import com.example.order.order.entity.OrderStatus;
import com.example.order.order.mapper.OrderMapper;
import com.example.order.product.entity.Product;
import com.example.order.product.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderMapper orderMapper;
    private final ProductService productService;
    private final InventoryService inventoryService;

    /**
     * Create order: validate product -> deduct stock -> create order
     */
    @Transactional(rollbackFor = Exception.class)
    public OrderInfo createOrder(CreateOrderRequest request) {
        // 1. Validate product exists and is on shelf
        Product product = productService.getAvailableProduct(request.getProductId());

        // 2. Deduct stock (throws if not enough)
        inventoryService.deductStock(request.getProductId(), request.getQuantity());

        // 3. Build order
        OrderInfo order = new OrderInfo();
        order.setOrderNo(generateOrderNo());
        order.setUserId(request.getUserId());
        order.setProductId(request.getProductId());
        order.setQuantity(request.getQuantity());
        order.setTotalAmount(product.getPrice().multiply(BigDecimal.valueOf(request.getQuantity())));
        order.setStatus(OrderStatus.PENDING.getValue());
        order.setExpireTime(LocalDateTime.now().plusMinutes(15));

        // 4. Save order
        orderMapper.insert(order);
        log.info("Order created: orderNo={}, userId={}, productId={}, quantity={}",
                order.getOrderNo(), order.getUserId(), order.getProductId(), order.getQuantity());

        return order;
    }

    /**
     * Query user's orders with pagination
     */
    public Page<OrderInfo> listUserOrders(Long userId, int pageNum, int pageSize) {
        Page<OrderInfo> page = new Page<>(pageNum, pageSize);
        LambdaQueryWrapper<OrderInfo> wrapper = new LambdaQueryWrapper<OrderInfo>()
                .eq(OrderInfo::getUserId, userId)
                .orderByDesc(OrderInfo::getCreateTime);
        return orderMapper.selectPage(page, wrapper);
    }

    /**
     * Get order detail by orderNo
     */
    public OrderInfo getByOrderNo(String orderNo) {
        OrderInfo order = orderMapper.selectOne(
                new LambdaQueryWrapper<OrderInfo>()
                        .eq(OrderInfo::getOrderNo, orderNo));
        if (order == null) {
            throw new BusinessException(ResultCode.ORDER_NOT_FOUND);
        }
        return order;
    }

    /**
     * Cancel order: only PENDING orders can be cancelled
     */
    @Transactional(rollbackFor = Exception.class)
    public OrderInfo cancelOrder(String orderNo) {
        OrderInfo order = getByOrderNo(orderNo);

        OrderStatus currentStatus = OrderStatus.fromValue(order.getStatus());
        if (!currentStatus.canTransitTo(OrderStatus.CANCELLED)) {
            throw new BusinessException(ResultCode.ORDER_STATUS_INVALID);
        }

        // 1. Update order status
        order.setStatus(OrderStatus.CANCELLED.getValue());

        // 2. Rollback stock
        inventoryService.rollbackStock(order.getProductId(), order.getQuantity());

        log.info("Order cancelled: orderNo={}", orderNo);
        return order;
    }

    /**
     * Generate unique order number: yyyyMMddHHmmss + 8-char UUID
     */
    private String generateOrderNo() {
        String timestamp = LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
        return "ORD" + timestamp + uuid;
    }
}