package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.retry.Retry;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * pub comp消息处理
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class PubCompHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @Override
    public Mono<Void> handle(MqttMessageContext<MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        mqttSession.onRecPubRespMessage();

        MqttMessage message = messageContext.getMessage();
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        int messageId = variableHeader.messageId();
        return Mono.fromRunnable(() ->
                Optional.ofNullable(brokerContext.getRetryService().getRetry(mqttSession.genMqttMessageRetryId(MqttMessageType.PUBREL, messageId)))
                        .ifPresent(Retry::cancel));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBCOMP;
    }
}
