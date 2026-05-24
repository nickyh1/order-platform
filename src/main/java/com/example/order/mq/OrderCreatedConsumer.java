package com.example.order.mq;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.order.config.RabbitMQConfig;
import com.example.order.mq.entity.OrderMessageLog;
import com.example.order.mq.mapper.OrderMessageLogMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreatedConsumer {

    private final OrderMessageLogMapper messageLogMapper;

    @RabbitListener(queues = RabbitMQConfig.ORDER_CREATED_QUEUE)
    public void onOrderCreated(Message message) {
        String payload = new String(message.getBody());
        String messageId = (String) message.getMessageProperties().getHeader("messageId");
        log.info("Received ORDER_CREATED: messageId={}", messageId);

        // Consumer idempotent: check by messageId (uses unique index, fast)
        if (messageId != null) {
            OrderMessageLog msgLog = messageLogMapper.selectOne(
                    new LambdaQueryWrapper<OrderMessageLog>()
                            .eq(OrderMessageLog::getMessageId, messageId)
            );

            if (msgLog != null && "CONSUMED".equals(msgLog.getStatus())) {
                log.info("Message already consumed, skipping: messageId={}", messageId);
                return;
            }

            // Process business logic
            log.info("Processing ORDER_CREATED: {}", payload);

            // Mark as consumed
            if (msgLog != null) {
                msgLog.setStatus("CONSUMED");
                messageLogMapper.updateById(msgLog);
            }
        }
    }
}