package com.example.order.mq;

import com.example.order.config.RabbitMQConfig;
import com.example.order.inventory.service.InventoryService;
import com.example.order.order.entity.OrderInfo;
import com.example.order.order.entity.OrderStatus;
import com.example.order.order.mapper.OrderMapper;
import com.example.order.order.service.OrderService;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutConsumer {

    private final OrderService orderService;
    private final OrderMapper orderMapper;
    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    @RabbitListener(queues = RabbitMQConfig.ORDER_TIMEOUT_QUEUE)
    @Transactional(rollbackFor = Exception.class)
    public void onOrderTimeout(String payload) {
        log.info("Received ORDER_TIMEOUT message: {}", payload);

        try {
            JsonNode node = objectMapper.readTree(payload);
            String orderNo = node.get("orderNo").asText();
            Long productId = node.get("productId").asLong();
            int quantity = node.get("quantity").asInt();

            OrderInfo order = orderService.getByOrderNo(orderNo);
            OrderStatus currentStatus = OrderStatus.fromValue(order.getStatus());

            // Only process if still PENDING
            if (currentStatus != OrderStatus.PENDING) {
                log.info("Order {} already in state {}, skip timeout", orderNo, currentStatus);
                return;
            }

            // Update order status to TIMEOUT
            order.setStatus(OrderStatus.TIMEOUT.getValue());
            orderMapper.updateById(order);

            // Rollback stock
            inventoryService.rollbackStock(productId, quantity);

            log.info("Order timeout processed: orderNo={}, stock rolled back", orderNo);

        } catch (Exception e) {
            log.error("Failed to process order timeout: {}", payload, e);
            throw new RuntimeException("Timeout processing failed", e);
        }
    }
}