package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import reactor.core.publisher.Mono;

/**
 * 虚拟mqtt session, 仅提供必要参数, 主要目的是
 * 充当{@link org.kin.mqtt.broker.core.message.MqttMessageHandler#handle(MqttMessageContext, MqttSession, MqttBrokerContext)}里面的MqttSession参数
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public class VirtualMqttSession extends MqttSession {
    public VirtualMqttSession(MqttBrokerContext brokerContext, String clientId) {
        super(brokerContext, null);
        super.clientId = clientId;
    }

    @Override
    public Mono<Void> sendMessage(MqttMessage mqttMessage, boolean retry) {
        //do nothing
        return Mono.empty();
    }

    @Override
    public Mono<Void> cacheQos2Message(int messageId, MqttMessageContext<MqttPublishMessage> messageContext) {
        //do nothing
        return Mono.empty();
    }

    @Override
    public boolean isVirtualSession() {
        return true;
    }

    @Override
    public boolean isChannelActive() {
        return true;
    }

    @Override
    public boolean isChannelWritable() {
        return true;
    }
}
