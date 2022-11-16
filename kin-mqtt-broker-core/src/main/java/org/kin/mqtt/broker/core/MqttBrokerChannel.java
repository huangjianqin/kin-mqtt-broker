package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import reactor.core.publisher.Mono;

/**
 * 来自于broker的publish消息处理时, 充当{@link org.kin.mqtt.broker.core.message.MqttMessageHandler#handle(MqttMessageWrapper, MqttChannel, MqttBrokerContext)}
 * 里面的MqttChannel参数
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public final class MqttBrokerChannel extends MqttChannel {
    public MqttBrokerChannel(MqttBrokerContext brokerContext, String clientId) {
        super(brokerContext, null);
        super.clientId = clientId;
    }

    @Override
    public Mono<Void> sendMessage(MqttMessage mqttMessage, boolean retry) {
        //do nothing
        return Mono.empty();
    }

    @Override
    public Mono<Void> cacheQos2Message(int messageId, MqttPublishMessage publishMessage) {
        //do nothing
        return Mono.empty();
    }

    @Override
    public boolean isBrokerChannel() {
        return true;
    }
}
