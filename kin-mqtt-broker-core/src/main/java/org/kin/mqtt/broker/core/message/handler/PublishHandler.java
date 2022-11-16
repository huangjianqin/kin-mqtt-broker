package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.store.MqttMessageStore;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
@Extension("publish")
public final class PublishHandler extends AbstractMqttMessageHandler<MqttPublishMessage> {
    private static final Logger log = LoggerFactory.getLogger(PublishHandler.class);

    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttPublishMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        try {
            // TODO: 2022/11/15
//            MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.PUBLISH_EVENT).increment();
            String clientId = mqttChannel.getClientId();
            MqttPublishMessage message = wrapper.getMessage();
            long timestamp = wrapper.getTimestamp();

            // TODO: 2022/11/15 acl检查
//            AclManager aclManager = receiveContext.getAclManager();
//            if (!mqttChannel.getIsMock() && !aclManager.check(mqttChannel, message.variableHeader().topicName(), AclAction.PUBLISH)) {
//                log.warn("mqtt【{}】publish topic 【{}】 acl not authorized ", mqttChannel.getConnection(), message.variableHeader().topicName());
//                return Mono.empty();
//            }

            TopicManager topicManager = brokerContext.getTopicManager();
            MqttMessageStore messageStore = brokerContext.getMessageStore();

            MqttPublishVariableHeader variableHeader = message.variableHeader();
            int packetId = variableHeader.packetId();
            MqttQoS qos = message.fixedHeader().qosLevel();

            //已注册的订阅
            Set<TopicSubscription> subscriptions = topicManager.getSubscriptions(variableHeader.topicName(), qos);
            switch (qos) {
                case AT_MOST_ONCE:
                    return broadcastPublish(brokerContext, subscriptions, message, timestamp)
                            .then(trySaveRetainMessage(messageStore, clientId, message, timestamp));
                case AT_LEAST_ONCE:
                    return broadcastPublish(brokerContext, subscriptions, message, timestamp)
                            .then(mqttChannel.sendMessage(MqttMessageUtils.createPubAck(packetId), false))
                            .then(trySaveRetainMessage(messageStore, clientId, message, timestamp));
                case EXACTLY_ONCE:
                    if (!mqttChannel.existQos2Message(packetId)) {
                        //Mqtt client -> broker: publish
                        //Mqtt client <- broker: pub rec
                        //Mqtt client -> broker: pub rel
                        //Mqtt client <- broker: publish
                        //...
                        //Mqtt client <- broker: pub comp
                        return mqttChannel
                                .cacheQos2Message(packetId, MqttMessageUtils.wrapPublish(message, qos, 0))
                                .then(mqttChannel.sendMessage(MqttMessageUtils.createPubRec(packetId), true));
                    }
                default:
                    return Mono.empty();
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return Mono.empty();
    }

    /**
     * 广播publish消息
     *
     * @param brokerContext broker context
     * @param subscriptions 已注册的订阅
     * @param message       接收到的publish消息
     * @param timestamp     mqtt消息接收时间戳
     * @return complete signal
     */
    private Mono<Void> broadcastPublish(MqttBrokerContext brokerContext, Set<TopicSubscription> subscriptions,
                                        MqttPublishMessage message, long timestamp) {
        return Mono.when(
                subscriptions.stream()
                        .filter(subscription -> filterOfflineSession(subscription.getMqttChannel(), brokerContext.getMessageStore(), () -> message, timestamp))
                        .map(subscription -> {
                            MqttQoS qoS = subscription.getQoS();
                            MqttChannel mqttChannel = subscription.getMqttChannel();
                            return mqttChannel.sendMessage(MqttMessageUtils.wrapPublish(message, qoS, mqttChannel.nextMessageId()), qoS.value() > 0);
                        })
                        .collect(Collectors.toList()));

    }

    /**
     * 尝试存储retain消息
     *
     * @param message      publish消息
     * @param messageStore 外部消息存储
     * @return complete signal
     */
    private Mono<Void> trySaveRetainMessage(MqttMessageStore messageStore, String clientId, MqttPublishMessage message, long timestamp) {
        return Mono.fromRunnable(() -> {
            if (message.fixedHeader().isRetain()) {
                //存储retain消息
                messageStore.saveRetainMessage(MqttMessageReplica.fromPublishMessage(clientId, message, timestamp));
            }
        });
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBLISH;
    }
}
