package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
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
    public Mono<Void> handle(MqttMessageContext<MqttConnAckMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        //暂时do nothing
        return Mono.empty();
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.CONNACK;
    }
}
