package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * conn ack消息处理
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class ConnAckHandler extends AbstractMqttMessageHandler<MqttConnAckMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttConnAckMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        //暂时do nothing
        return Mono.empty();
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.CONNACK;
    }
}
