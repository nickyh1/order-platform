package com.example.order.mq;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.order.config.RabbitMQConfig;
import com.example.order.inventory.service.InventoryService;
import com.example.order.mq.entity.OrderMessageLog;
import com.example.order.mq.mapper.OrderMessageLogMapper;
import com.example.order.order.entity.OrderInfo;
import com.example.order.order.entity.OrderStatus;
import com.example.order.order.mapper.OrderMapper;
import com.example.order.order.service.OrderService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
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
    private final OrderMessageLogMapper messageLogMapper;
    private final ObjectMapper objectMapper;

    @RabbitListener(queues = RabbitMQConfig.ORDER_TIMEOUT_QUEUE)
    @Transactional(rollbackFor = Exception.class)
    public void onOrderTimeout(Message message) {
        String payload = new String(message.getBody());
        String messageId = (String) message.getMessageProperties().getHeader("messageId");
        log.info("Received ORDER_TIMEOUT: messageId={}", messageId);

        try {
            // Consumer idempotent: check by messageId
            OrderMessageLog msgLog = null;
            if (messageId != null) {
                msgLog = messageLogMapper.selectOne(
                        new LambdaQueryWrapper<OrderMessageLog>()
                                .eq(OrderMessageLog::getMessageId, messageId)
                );

                if (msgLog != null && "CONSUMED".equals(msgLog.getStatus())) {
                    log.info("Timeout message already consumed, skipping: messageId={}", messageId);
                    return;
                }
            }

            // Parse payload
            JsonNode node = objectMapper.readTree(payload);
            String orderNo = node.get("orderNo").asText();
            Long productId = node.get("productId").asLong();
            int quantity = node.get("quantity").asInt();

            // Check order status
            OrderInfo order = orderService.getByOrderNo(orderNo);
            OrderStatus currentStatus = OrderStatus.fromValue(order.getStatus());

            if (currentStatus != OrderStatus.PENDING) {
                log.info("Order {} already in state {}, skip timeout", orderNo, currentStatus);
                if (msgLog != null) {
                    msgLog.setStatus("CONSUMED");
                    messageLogMapper.updateById(msgLog);
                }
                return;
            }

            // Timeout: update order + rollback stock
            order.setStatus(OrderStatus.TIMEOUT.getValue());
            orderMapper.updateById(order);
            inventoryService.rollbackStock(productId, quantity);

            // Mark consumed
            if (msgLog != null) {
                msgLog.setStatus("CONSUMED");
                messageLogMapper.updateById(msgLog);
            }

            log.info("Order timeout processed: orderNo={}, messageId={}", orderNo, messageId);

        } catch (Exception e) {
            log.error("Failed to process order timeout: messageId={}", messageId, e);
            throw new RuntimeException("Timeout processing failed", e);
        }
    }
}