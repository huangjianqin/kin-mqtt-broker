package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * @author huangjianqin
 * @date 2022/11/14
 */
@Extension("unsubscribe")
public final class UnsubscribeHandler extends AbstractMqttMessageHandler<MqttUnsubscribeMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttUnsubscribeMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        MqttUnsubscribeMessage message = wrapper.getMessage();
        return Mono.fromRunnable(() -> {
            // TODO: 2022/11/15
//            MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.UN_SUBSCRIBE_EVENT).increment();
            TopicManager topicManager = brokerContext.getTopicManager();
            message.payload()
                    .topics()
                    .stream()
                    .map(topic -> TopicSubscription.forRemove(topic, mqttChannel))
                    .forEach(topicManager::removeSubscription);
        }).then(mqttChannel.sendMessage(MqttMessageUtils.createUnsubAck(message.variableHeader().messageId()), false));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.UNSUBSCRIBE;
    }
}
