package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * 心跳ping消息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class PingReqHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        return mqttChannel.sendMessage(MqttMessageUtils.createPingResp(), false);
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PINGREQ;
    }
}
