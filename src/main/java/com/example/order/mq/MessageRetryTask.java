package com.example.order.mq;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.order.config.RabbitMQConfig;
import com.example.order.monitor.OrderMetrics;
import com.example.order.mq.entity.OrderMessageLog;
import com.example.order.mq.mapper.OrderMessageLogMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageRetryTask {

    private final OrderMessageLogMapper messageLogMapper;
    private final RabbitTemplate rabbitTemplate;
    private final OrderMetrics orderMetrics;

    /**
     * Every 30 seconds, scan PENDING messages and retry sending.
     * Status is NOT updated to SENT here — MqConfirmCallback handles that after broker ack.
     */
    @Scheduled(fixedDelay = 30000)
    public void retryPendingMessages() {
        List<OrderMessageLog> pendingMessages = messageLogMapper.selectList(
                new LambdaQueryWrapper<OrderMessageLog>()
                        .in(OrderMessageLog::getStatus, "PENDING", "FAILED")
                        .le(OrderMessageLog::getNextRetryTime, LocalDateTime.now())
                        .lt(OrderMessageLog::getRetryCount, 3)
        );

        if (pendingMessages.isEmpty()) {
            return;
        }

        log.info("Found {} pending messages to retry", pendingMessages.size());

        for (OrderMessageLog msg : pendingMessages) {
            try {
                String routingKey = getRoutingKey(msg.getMessageType());

                // Send with CorrelationData so MqConfirmCallback can update status to SENT on broker ack
                CorrelationData correlationData = new CorrelationData(msg.getMessageId());
                rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_EXCHANGE, routingKey, msg.getPayload(),
                        message -> {
                            message.getMessageProperties().setHeader("messageId", msg.getMessageId());
                            return message;
                        }, correlationData);

                // Only advance retry counter and next check time — SENT is set by MqConfirmCallback on ack
                msg.setRetryCount(msg.getRetryCount() + 1);
                msg.setNextRetryTime(LocalDateTime.now().plusSeconds(30L * (msg.getRetryCount() + 1)));
                messageLogMapper.updateById(msg);
                orderMetrics.getMessageRetryCounter().increment();
                log.info("Retry sent, awaiting broker confirm: messageId={}, type={}, retryCount={}",
                        msg.getMessageId(), msg.getMessageType(), msg.getRetryCount());

            } catch (Exception e) {
                msg.setRetryCount(msg.getRetryCount() + 1);
                msg.setNextRetryTime(LocalDateTime.now().plusSeconds(30L * msg.getRetryCount()));

                if (msg.getRetryCount() >= msg.getMaxRetry()) {
                    msg.setStatus("FAILED");
                    log.error("Message exhausted max retries: messageId={}, type={}", msg.getMessageId(), msg.getMessageType());
                }

                messageLogMapper.updateById(msg);
            }
        }
    }

    private String getRoutingKey(String messageType) {
        return switch (messageType) {
            case "ORDER_CREATED" -> RabbitMQConfig.RK_ORDER_CREATED;
            case "PAYMENT_CALLBACK" -> RabbitMQConfig.RK_ORDER_PAYMENT;
            case "ORDER_TIMEOUT" -> "order.delay";
            default -> throw new IllegalArgumentException("Unknown message type: " + messageType);
        };
    }
}
