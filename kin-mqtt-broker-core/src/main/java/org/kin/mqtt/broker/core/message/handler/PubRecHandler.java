package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.Retry;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * 接收到pub rec消息, 预期响应pub rel消息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class PubRecHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @Override
    public Mono<Void> handle(MqttMessageContext<MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        mqttSession.onRecPubRespMessage();

        MqttMessage message = messageContext.getMessage();
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        //publish消息id
        int messageId = variableHeader.messageId();
        long uuid = mqttSession.generateUuid(MqttMessageType.PUBLISH, messageId);
        //停止retry, 然后响应pub rel消息
        return Mono.fromRunnable(() -> Optional.ofNullable(brokerContext.getRetryService().getRetry(uuid)).ifPresent(Retry::cancel))
                .then(mqttSession.sendMessage(MqttMessageUtils.createPubRel(messageId), true));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBREC;
    }
}
