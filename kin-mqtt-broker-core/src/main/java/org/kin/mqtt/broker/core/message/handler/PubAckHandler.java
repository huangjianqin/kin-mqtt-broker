package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.retry.Retry;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * pub ack消息处理
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class PubAckHandler extends AbstractMqttMessageHandler<MqttPubAckMessage> {
    @Override
    public Mono<Void> handle(MqttMessageContext<MqttPubAckMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        return Mono.fromRunnable(() -> {
            mqttSession.onRecPubRespMessage();

            MqttMessage message = messageContext.getMessage();
            MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
            int messageId = variableHeader.messageId();
            Optional.ofNullable(brokerContext.getRetryService().getRetry(mqttSession.genMqttMessageRetryId(MqttMessageType.PUBLISH, messageId))).ifPresent(Retry::cancel);
        });
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBACK;
    }
}
