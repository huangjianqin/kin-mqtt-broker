package org.kin.mqtt.broker.core.message;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
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
     * @param messageContext mqtt message context
     * @param mqttSession    mqtt session
     * @param brokerContext  mqtt broker brokerContext
     * @return complete signal
     */
    Mono<Void> handle(MqttMessageContext<M> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext);

    /**
     * 获取该handler能处理额mqtt消息类型
     *
     * @return {@link MqttMessageType}
     */
    @Nonnull
    MqttMessageType getMqttMessageType();
}
