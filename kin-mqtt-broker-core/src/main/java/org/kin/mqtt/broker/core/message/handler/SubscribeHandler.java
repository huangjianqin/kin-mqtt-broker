package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.store.MqttMessageStore;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
@Extension("subscribe")
public final class SubscribeHandler extends AbstractMqttMessageHandler<MqttSubscribeMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttSubscribeMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        MqttSubscribeMessage message = wrapper.getMessage();
        // TODO: 2022/11/15
//        MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.SUBSCRIBE_EVENT).increment();
        return Mono.fromCallable(() -> {
            TopicManager topicManager = brokerContext.getTopicManager();
            Set<TopicSubscription> subscriptions =
                    message.payload().topicSubscriptions()
                            .stream()
                            // TODO: 2022/11/15 acl manager
//                            .filter(subscribeTopic -> aclManager.check(mqttChannel, subscribeTopic.getTopicFilter(), AclAction.SUBSCRIBE))
                            .map(subscription -> new TopicSubscription(subscription.topicName(), subscription.qualityOfService(), mqttChannel))
                            .collect(Collectors.toSet());
            //注册订阅
            if (CollectionUtils.isNonEmpty(subscriptions)) {
                topicManager.addSubscriptions(subscriptions);
            }

            return subscriptions;
        }).flatMap(subscriptions -> {
            MqttMessageStore messageStore = brokerContext.getMessageStore();
            int messageId = message.variableHeader().messageId();
            //响应subscribe的qos list
            List<Integer> respQosList = subscriptions.stream().map(s -> s.getQoS().value()).collect(Collectors.toList());
            //发送retain消息
            Flux<Void> sendRetainFlux = Flux.fromIterable(subscriptions)
                    .flatMap(subscription -> sendRetainMessage(messageStore, mqttChannel, subscription.getTopic()));
            return Mono.from(mqttChannel.sendMessage(MqttMessageUtils.createSubAck(messageId, respQosList), false))
                    .thenEmpty(sendRetainFlux);
        });
    }

    /**
     * 订阅成功后, 往mqtt client发送retain消息
     *
     * @param messageStore 外部消息存储
     * @param mqttChannel  mqtt client
     * @param topic        订阅的topic name
     */
    private Flux<Void> sendRetainMessage(MqttMessageStore messageStore, MqttChannel mqttChannel, String topic) {
        return messageStore.getRetainMessage(topic)
                .flatMap(retainMessage -> mqttChannel.sendMessage(MqttMessageUtils.createPublish(mqttChannel, retainMessage), retainMessage.getQos() > 0));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.SUBSCRIBE;
    }
}
