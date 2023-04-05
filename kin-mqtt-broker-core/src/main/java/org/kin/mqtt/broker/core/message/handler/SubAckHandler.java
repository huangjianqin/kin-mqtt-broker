package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * sub ack消息处理
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class SubAckHandler extends AbstractMqttMessageHandler<MqttSubAckMessage> {
    @Override
    public Mono<Void> handle(MqttMessageContext<MqttSubAckMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        //暂时do nothing
        return Mono.empty();
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.SUBACK;
    }
}
