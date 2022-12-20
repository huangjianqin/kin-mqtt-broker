package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.Retry;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * pub ack消息处理
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
@Extension("pubAck")
public class PubAckHandler extends AbstractMqttMessageHandler<MqttPubAckMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttPubAckMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        return Mono.fromRunnable(() -> {
            MqttMessage message = wrapper.getMessage();
            MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
            int messageId = variableHeader.messageId();
            Optional.ofNullable(brokerContext.getRetryService().getRetry(mqttChannel.generateUuid(MqttMessageType.PUBLISH, messageId))).ifPresent(Retry::cancel);
        });
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBACK;
    }
}
