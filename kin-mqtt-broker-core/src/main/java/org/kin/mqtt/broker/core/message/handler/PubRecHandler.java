package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.Retry;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * 接收到pub rec消息, 预期响应pub rel消息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
@Extension("pubRec")
public final class PubRecHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        MqttMessage message = wrapper.getMessage();
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        //publish消息id
        int messageId = variableHeader.messageId();
        long uuid = mqttChannel.generateUuid(MqttMessageType.PUBLISH, messageId);
        //停止retry, 然后响应pub rel消息
        return Mono.fromRunnable(() -> Optional.ofNullable(brokerContext.getRetryService().getRetry(uuid)).ifPresent(Retry::cancel))
                .then(mqttChannel.sendMessage(MqttMessageUtils.createPubRel(messageId), true));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBREC;
    }
}
