package com.example.order.mq;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.example.order.mq.entity.OrderMessageLog;
import com.example.order.mq.mapper.OrderMessageLogMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * Publisher confirm and returns callbacks.
 * Marks message SENT only after broker ack; on nack/return the message stays PENDING
 * so the retry task can resend it.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqConfirmCallback implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnsCallback {

    private final OrderMessageLogMapper messageLogMapper;

    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        if (correlationData == null) {
            return;
        }
        String messageId = correlationData.getId();
        if (ack) {
            messageLogMapper.update(null, new LambdaUpdateWrapper<OrderMessageLog>()
                    .eq(OrderMessageLog::getMessageId, messageId)
                    .set(OrderMessageLog::getStatus, "SENT"));
            log.info("[MQ-Confirm] Broker ack, message marked SENT: messageId={}", messageId);
        } else {
            // Keep status PENDING so retry task picks it up
            log.warn("[MQ-Confirm] Broker nack, retry task will handle: messageId={}, cause={}", messageId, cause);
        }
    }

    @Override
    public void returnedMessage(ReturnedMessage returned) {
        String messageId = (String) returned.getMessage().getMessageProperties().getHeader("messageId");
        // Keep status PENDING so retry task picks it up
        log.warn("[MQ-Return] Message unroutable, retry task will handle: messageId={}, exchange={}, routingKey={}, replyText={}",
                messageId, returned.getExchange(), returned.getRoutingKey(), returned.getReplyText());
    }
}
