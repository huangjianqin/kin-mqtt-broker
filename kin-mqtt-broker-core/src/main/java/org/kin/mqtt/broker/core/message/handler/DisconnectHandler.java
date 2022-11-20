package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import javax.annotation.Nonnull;

/**
 * disconnect消息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
@Extension("disconnect")
public final class DisconnectHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        return Mono.fromRunnable(() -> {
            Connection connection;
            if (!(connection = mqttChannel.getConnection()).isDisposed()) {
                connection.dispose();
            }

            // TODO: 2022/11/14
//            MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.DIS_CONNECT_EVENT).increment();
        });
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.DISCONNECT;
    }
}