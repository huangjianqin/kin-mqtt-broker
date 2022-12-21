package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
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
import java.util.List;
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
                .map(st -> new TopicSubscription(st.topicName(), st.qualityOfService(), mqttChannel))
                .collect(Collectors.toSet())
                .flatMap(subscriptions -> {
                    //注册订阅
                    if (CollectionUtils.isNonEmpty(subscriptions)) {
                        topicManager.addSubscriptions(subscriptions);
                    }

                    MqttMessageStore messageStore = brokerContext.getMessageStore();
                    int messageId = message.variableHeader().messageId();
                    //响应subscribe的qos list
                    List<Integer> respQosList = subscriptions.stream().map(s -> s.getQoS().value()).collect(Collectors.toList());
                    //发送retain消息
                    Flux<Void> sendRetainFlux = Flux.fromIterable(subscriptions)
                            .flatMap(subscription -> sendRetainMessage(messageStore, mqttChannel, subscription.getTopic()));
                    return Mono.from(mqttChannel.sendMessage(MqttMessageUtils.createSubAck(messageId, respQosList), false))
                            .thenEmpty(sendRetainFlux)
                            .then(Mono.fromRunnable(() -> brokerContext.broadcastEvent(new MqttSubscribeEvent(mqttChannel, subscriptions))))
                            .then(Mono.fromRunnable(() -> brokerContext.broadcastClusterEvent(
                                    SubscriptionsAddEvent.of(subscriptions.stream().map(TopicSubscription::getTopic).collect(Collectors.toList())))));
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
