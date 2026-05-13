package com.example.order.mq;

import com.example.order.config.RabbitMQConfig;
import com.example.order.mq.entity.OrderMessageLog;
import com.example.order.mq.mapper.OrderMessageLogMapper;
import org.apache.ibatis.annotations.Mapper;
import tools.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
@Mapper
public class OrderMessageProducer {

    private final RabbitTemplate rabbitTemplate;
    private final OrderMessageLogMapper messageLogMapper;
    private final ObjectMapper objectMapper;

    /**
     * Save message to local outbox table (called INSIDE the order transaction).
     * Returns the message log so caller can send it after transaction commits.
     */
    public OrderMessageLog saveMessageLog(Long orderId, String messageType, Map<String, Object> payload) {
        try {
            OrderMessageLog msgLog = new OrderMessageLog();
            msgLog.setMessageId(UUID.randomUUID().toString().replace("-", ""));
            msgLog.setOrderId(orderId);
            msgLog.setMessageType(messageType);
            msgLog.setPayload(objectMapper.writeValueAsString(payload));
            msgLog.setStatus("PENDING");
            msgLog.setRetryCount(0);
            msgLog.setMaxRetry(3);
            msgLog.setNextRetryTime(LocalDateTime.now());
            messageLogMapper.insert(msgLog);
            log.info("Message log saved: messageId={}, type={}, orderId={}", msgLog.getMessageId(), messageType, orderId);
            return msgLog;
        } catch (Exception e) {
            log.error("Failed to save message log: orderId={}, type={}", orderId, messageType, e);
            throw new RuntimeException("Failed to save message log", e);
        }
    }

    /**
     * Send message to RabbitMQ and update log status.
     * Called AFTER transaction commits.
     */
    public void sendMessage(OrderMessageLog msgLog, String routingKey) {
        try {
            rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_EXCHANGE, routingKey, msgLog.getPayload());
            msgLog.setStatus("SENT");
            messageLogMapper.updateById(msgLog);
            log.info("Message sent: messageId={}, routingKey={}", msgLog.getMessageId(), routingKey);
        } catch (Exception e) {
            log.error("Failed to send message: messageId={}, will be retried", msgLog.getMessageId(), e);
            // Leave status as PENDING, retry task will pick it up
        }
    }

    /**
     * Send order to delay queue (for timeout detection).
     */
    public void sendDelayMessage(OrderMessageLog msgLog) {
        try {
            rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_EXCHANGE, "order.delay", msgLog.getPayload());
            msgLog.setStatus("SENT");
            messageLogMapper.updateById(msgLog);
            log.info("Delay message sent: messageId={}, orderId={}", msgLog.getMessageId(), msgLog.getOrderId());
        } catch (Exception e) {
            log.error("Failed to send delay message: messageId={}", msgLog.getMessageId(), e);
        }
    }
}