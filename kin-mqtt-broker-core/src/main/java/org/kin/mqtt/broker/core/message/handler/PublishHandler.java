package org.kin.mqtt.broker.core.message.handler;

import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.acl.AclAction;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.metrics.MetricsNames;
import org.kin.mqtt.broker.store.MqttMessageStore;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
public class PublishHandler extends AbstractMqttMessageHandler<MqttPublishMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttPublishMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        if (wrapper.isFromCluster()) {
            Metrics.counter(MetricsNames.CLUSTER_PUBLISH_MSG_COUNT).increment();
        } else {
            Metrics.counter(MetricsNames.PUBLISH_MSG_COUNT).increment();
        }

        //acl访问权限检查
        AclService aclService = brokerContext.getAclService();
        if (mqttChannel.isVirtualChannel()) {
            return handle0(wrapper, mqttChannel, brokerContext);
        } else {
            MqttPublishMessage message = wrapper.getMessage();
            MqttPublishVariableHeader variableHeader = message.variableHeader();
            String topicName = variableHeader.topicName();
            return aclService.checkPermission(mqttChannel.getHost(), mqttChannel.getClientId(), topicName, AclAction.PUBLISH)
                    .flatMap(aclResult -> {
                        if (aclResult) {
                            //允许访问
                            return handle0(wrapper, mqttChannel, brokerContext);
                        } else {
                            return Mono.error(new IllegalStateException(String.format("mqtt publish message for topic '%s' acl is not allowed, %s", topicName, message)));
                        }
                    });
        }
    }

    private Mono<Void> handle0(MqttMessageWrapper<MqttPublishMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        String clientId = mqttChannel.getClientId();
        MqttPublishMessage message = wrapper.getMessage();
        long timestamp = wrapper.getTimestamp();

        TopicManager topicManager = brokerContext.getTopicManager();
        MqttMessageStore messageStore = brokerContext.getMessageStore();

        MqttPublishVariableHeader variableHeader = message.variableHeader();
        int packetId = variableHeader.packetId();
        MqttQoS qos = message.fixedHeader().qosLevel();

        //已注册的订阅
        Set<TopicSubscription> subscriptions = topicManager.getSubscriptions(variableHeader.topicName(), qos);
        if (mqttChannel.isVirtualChannel()) {
            //其他集群广播 | 消息重发(rule) 接收到的publish消息
            return broadcastPublish(brokerContext, subscriptions, message, clientId, timestamp);
        }

        switch (qos) {
            case AT_MOST_ONCE:
                return broadcastPublish(brokerContext, subscriptions, message, clientId, timestamp);
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
    }

    /**
     * 广播publish消息
     *
     * @param brokerContext broker context
     * @param subscriptions 已注册的订阅
     * @param message       接收到的publish消息
     * @param clientId      mqtt sender id
     * @param timestamp     mqtt消息接收时间戳
     * @return complete signal
     */
    private Mono<Void> broadcastPublish(MqttBrokerContext brokerContext, Set<TopicSubscription> subscriptions,
                                        MqttPublishMessage message, String clientId, long timestamp) {
        return broadcastPublish(brokerContext, subscriptions, message, timestamp)
                .then(trySaveRetainMessage(brokerContext.getMessageStore(), clientId, message, timestamp));
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
                messageStore.saveRetainMessage(MqttMessageUtils.toReplica(clientId, message, timestamp));
            }
        });
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBLISH;
    }
}
