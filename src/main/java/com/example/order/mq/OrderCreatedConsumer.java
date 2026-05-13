package com.example.order.mq;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.order.config.RabbitMQConfig;
import com.example.order.mq.entity.OrderMessageLog;
import com.example.order.mq.mapper.OrderMessageLogMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreatedConsumer {

    private final OrderMessageLogMapper messageLogMapper;

    @RabbitListener(queues = RabbitMQConfig.ORDER_CREATED_QUEUE)
    public void onOrderCreated(String payload) {
        log.info("Received ORDER_CREATED message: {}", payload);

        // Consumer idempotent: check if already consumed
        // Find the message log by matching payload (in production, use messageId in header)
        OrderMessageLog msgLog = messageLogMapper.selectOne(
                new LambdaQueryWrapper<OrderMessageLog>()
                        .eq(OrderMessageLog::getMessageType, "ORDER_CREATED")
                        .eq(OrderMessageLog::getPayload, payload)
                        .last("LIMIT 1")
        );

        if (msgLog != null && "CONSUMED".equals(msgLog.getStatus())) {
            log.info("Message already consumed, skipping: messageId={}", msgLog.getMessageId());
            return;
        }

        // Process business logic here
        // e.g., notify warehouse, send confirmation email, etc.
        log.info("Processing ORDER_CREATED: {}", payload);

        // Mark as consumed
        if (msgLog != null) {
            msgLog.setStatus("CONSUMED");
            messageLogMapper.updateById(msgLog);
        }
    }
}