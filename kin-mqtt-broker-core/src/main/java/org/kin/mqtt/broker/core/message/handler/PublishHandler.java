package org.kin.mqtt.broker.core.message.handler;

import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.kin.mqtt.broker.acl.AclAction;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.PubTopic;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.metrics.MetricsNames;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.mqtt.broker.utils.TopicUtils;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
public class PublishHandler extends AbstractMqttMessageHandler<MqttPublishMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttPublishMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        //单个连接消息速率整型
        mqttChannel.checkPubMessageRate();

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
            return aclService.checkPermission(mqttChannel.getHost(), mqttChannel.getClientId(), mqttChannel.getUserName(), topicName, AclAction.PUBLISH)
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

        MqttMessageStore messageStore = brokerContext.getMessageStore();

        MqttPublishVariableHeader variableHeader = message.variableHeader();
        int packetId = variableHeader.packetId();
        MqttQoS qos = message.fixedHeader().qosLevel();

        PubTopic pubTopic = TopicUtils.parsePubTopic(variableHeader.topicName());

        if (mqttChannel.isVirtualChannel()) {
            //其他集群广播 | 消息重发(rule) 接收到的publish消息
            return broadcastPublish(brokerContext, mqttChannel, pubTopic, wrapper);
        }

        switch (qos) {
            case AT_MOST_ONCE:
                return broadcastPublish(brokerContext, mqttChannel, pubTopic, wrapper);
            case AT_LEAST_ONCE:
                return broadcastPublish0(brokerContext, mqttChannel, pubTopic, wrapper)
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
                            .cacheQos2Message(packetId,
                                    //暂不移除topic中delayed相关信息, pub rel时再移除
                                    new MqttMessageWrapper<>(wrapper, MqttMessageUtils.wrapPublish(message, qos, 0)))
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
     * @param pubTopic      解析publish消息的topic name
     * @param wrapper       接收到的mqtt message wrapper
     * @param sender        mqtt message sender
     * @return complete signal
     */
    private Mono<Void> broadcastPublish(MqttBrokerContext brokerContext, MqttChannel sender, PubTopic pubTopic,
                                        MqttMessageWrapper<MqttPublishMessage> wrapper) {
        return broadcastPublish0(brokerContext, sender, pubTopic, wrapper)
                .then(trySaveRetainMessage(brokerContext.getMessageStore(), sender.getClientId(), wrapper.getMessage(), wrapper.getTimestamp()));
    }

    /**
     * 广播publish消息
     *
     * @param brokerContext broker context
     * @param pubTopic      解析publish消息的topic name
     * @param wrapper       接收到的mqtt message wrapper
     * @param sender        mqtt message sender
     * @return complete signal
     */
    private Mono<Void> broadcastPublish0(MqttBrokerContext brokerContext, MqttChannel sender,
                                         PubTopic pubTopic, MqttMessageWrapper<MqttPublishMessage> wrapper) {
        int delay = pubTopic.getDelay();
        if (delay > 0) {
            HashedWheelTimer bsTimer = brokerContext.getBsTimer();
            //reference count+1
            //ack后payload会被touch
            wrapper.getMessage().payload().retain();
            Timeout timeout = bsTimer.newTimeout(t ->
                            broadcastPublish1(brokerContext, sender, pubTopic, wrapper)
                                    .then(Mono.fromRunnable(() -> sender.removeDelayPubTimeout(t)))
                                    .subscribe(),
                    delay, TimeUnit.SECONDS);
            sender.addDelayPubTimeout(timeout);
            return Mono.empty();
        } else {
            return broadcastPublish1(brokerContext, sender, pubTopic, wrapper);
        }
    }

    /**
     * 广播publish消息
     *
     * @param brokerContext broker context
     * @param pubTopic      解析publish消息的topic name
     * @param wrapper       接收到的mqtt message wrapper
     * @param sender        mqtt message sender
     * @return complete signal
     */
    private Mono<Void> broadcastPublish1(MqttBrokerContext brokerContext, MqttChannel sender,
                                         PubTopic pubTopic, MqttMessageWrapper<MqttPublishMessage> wrapper) {
        MqttFixedHeader fixedHeader = wrapper.getMessage().fixedHeader();
        MqttQoS qos = fixedHeader.qosLevel();

        Set<TopicSubscription> subscriptions = brokerContext.getTopicManager().getSubscriptions(pubTopic.getName(), qos, sender);

        return Mono.when(subscriptions.stream()
                .filter(subscription -> filterOfflineSession(subscription.getMqttChannel(), brokerContext.getMessageStore(),
                        wrapper::getMessage, wrapper.getTimestamp()))
                .filter(s -> !wrapper.isExpire())
                .map(subscription -> {
                    MqttChannel mqttChannel = subscription.getMqttChannel();
                    //如果是delayed topic, 移除topic中delayed相关信息
                    return mqttChannel.sendMessage(MqttMessageUtils.wrapPublish(wrapper.getMessage(), subscription, pubTopic.getName(), mqttChannel.nextMessageId()),
                            subscription.getQoS().value() > 0);
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
