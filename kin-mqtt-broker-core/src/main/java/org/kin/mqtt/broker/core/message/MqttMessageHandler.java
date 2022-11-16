package org.kin.mqtt.broker.core.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * mqtt消息处理逻辑
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public interface MqttMessageHandler<M extends MqttMessage> {
    /**
     * mqtt消息处理逻辑
     *
     * @param wrapper       mqtt message wrapper
     * @param mqttChannel   mqtt channel
     * @param brokerContext mqtt broker brokerContext
     * @return complete signal
     */
    Mono<Void> handle(MqttMessageWrapper<M> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext);

    /**
     * 获取该handler能处理额mqtt消息类型
     *
     * @return {@link MqttMessageType}
     */
    @Nonnull
    MqttMessageType getMqttMessageType();
}
