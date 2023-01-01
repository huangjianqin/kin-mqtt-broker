package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.acl.AclAction;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.cluster.event.SubscriptionsAddEvent;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.event.MqttSubscribeEvent;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
public class SubscribeHandler extends AbstractMqttMessageHandler<MqttSubscribeMessage> {
    private static final Logger log = LoggerFactory.getLogger(SubscribeHandler.class);

    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttSubscribeMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        MqttSubscribeMessage message = wrapper.getMessage();

        AclService aclService = brokerContext.getAclService();
        TopicManager topicManager = brokerContext.getTopicManager();
        return Flux.fromIterable(message.payload().topicSubscriptions())
                .filterWhen(st -> aclService.checkPermission(mqttChannel.getHost(), mqttChannel.getClientId(), st.topicName(), AclAction.SUBSCRIBE))
                .collect(Collectors.toSet())
                //netty的topic subscription定义
                .flatMap(rawSubscriptions -> {
                    //转换
                    Set<TopicSubscription> subscriptions = new HashSet<>(rawSubscriptions.size());
                    Map<String, MqttTopicSubscription> topic2RawTs = new HashMap<>(4);
                    for (MqttTopicSubscription rawSubscription : rawSubscriptions) {
                        TopicSubscription subscription = new TopicSubscription(rawSubscription.topicName(), rawSubscription.qualityOfService(), mqttChannel);
                        subscriptions.add(subscription);
                        topic2RawTs.put(subscription.getTopic(), rawSubscription);
                    }
                    //注册订阅
                    Set<String> filteredTopics = Collections.emptySet();
                    if (CollectionUtils.isNonEmpty(subscriptions)) {
                        //过滤已注册的同时替换旧订阅的qos
                        Set<TopicSubscription> filteredTopicSubscriptions = mqttChannel.filterRegisteredTopicSubscriptions(subscriptions);
                        filteredTopics = filteredTopicSubscriptions.stream()
                                .map(TopicSubscription::getTopic)
                                .collect(Collectors.toSet());
                        //注册新订阅
                        topicManager.addSubscriptions(filteredTopicSubscriptions);
                    }

                    MqttMessageStore messageStore = brokerContext.getMessageStore();
                    int messageId = message.variableHeader().messageId();
                    //响应subscribe的qos list
                    List<Integer> respQosList = subscriptions.stream().map(s -> s.getQoS().value()).collect(Collectors.toList());
                    //发送retain消息
                    Set<String> finalFilteredTopics = filteredTopics;
                    Flux<Void> sendRetainFlux = Flux.fromIterable(topic2RawTs.entrySet())
                            .flatMap(entry -> sendRetainMessage(messageStore, mqttChannel, finalFilteredTopics, entry.getKey(), entry.getValue()));
                    //response sub ack
                    return Mono.from(mqttChannel.sendMessage(MqttMessageUtils.createSubAck(messageId, respQosList), false))
                            //send retain message
                            .thenEmpty(sendRetainFlux)
                            //broadcast mqtt event
                            .then(Mono.fromRunnable(() -> brokerContext.broadcastEvent(new MqttSubscribeEvent(mqttChannel, subscriptions))))
                            .then(Mono.fromRunnable(() -> brokerContext.broadcastClusterEvent(
                                    SubscriptionsAddEvent.of(subscriptions.stream().map(TopicSubscription::getTopic).collect(Collectors.toList())))));
                });
    }

    /**
     * 订阅成功后, 往mqtt client发送retain消息
     *
     * @param messageStore    外部消息存储
     * @param mqttChannel     mqtt client
     * @param topic           订阅的topic name
     * @param rawSubscription 原始订阅信息
     */
    private Flux<Void> sendRetainMessage(MqttMessageStore messageStore, MqttChannel mqttChannel, Set<String> filteredTopics,
                                         String topic, MqttTopicSubscription rawSubscription) {
        MqttSubscriptionOption option = rawSubscription.option();
        MqttSubscriptionOption.RetainedHandlingPolicy retainedHandlingPolicy = option.retainHandling();

        if (Objects.isNull(retainedHandlingPolicy)) {
            return sendRetainMessage(messageStore, mqttChannel, topic);
        }

        switch (retainedHandlingPolicy) {
            case SEND_AT_SUBSCRIBE:
                //只要客户端订阅成功, 服务端就发送保留消息
                return sendRetainMessage(messageStore, mqttChannel, topic);
            case SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS:
                //客户端订阅成功且该订阅此前不存在, 服务端才发送保留消息. 毕竟有些时候客户端重新发起订阅可能只是为了改变一下 QoS, 并不意味着它想再次接收保留消息
                if (!filteredTopics.contains(topic)) {
                    //之前已订阅
                    return sendRetainMessage(messageStore, mqttChannel, topic);
                } else {
                    return Flux.empty();
                }
            case DONT_SEND_AT_SUBSCRIBE:
                //即便客户订阅成功, 服务端也不会发送保留消息
                return Flux.empty();
            default:
                throw new IllegalStateException(String.format("unknown retained handling policy '%s'", retainedHandlingPolicy));
        }
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
                //以往异常导致正常流程无法继续
                .onErrorResume(t -> {
                    log.error("", t);
                    return Flux.empty();
                })
                .flatMap(retainMessage -> mqttChannel.sendMessage(MqttMessageUtils.createPublish(mqttChannel, retainMessage), retainMessage.getQos() > 0));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.SUBSCRIBE;
    }
}
