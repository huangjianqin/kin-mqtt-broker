package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.*;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.Retry;
import org.kin.mqtt.broker.core.RetryService;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.PubTopic;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.mqtt.broker.utils.TopicUtils;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 接收到pub rel消息, 预期响应pub comp消息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class PubRelHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        MqttMessage message = wrapper.getMessage();
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        int messageId = variableHeader.messageId();
        MqttMessageWrapper<MqttPublishMessage> pubWrapper = mqttChannel.removeQos2Message(messageId);
        if (Objects.nonNull(pubWrapper)) {
            if (pubWrapper.isExpire()) {
                //过期, 回复pub comp
                return mqttChannel.sendMessage(MqttMessageUtils.createPubComp(messageId), false);
            } else {
                MqttPublishMessage qos2Message = pubWrapper.getMessage();
                PubTopic pubTopic = TopicUtils.parsePubTopic(qos2Message.variableHeader().topicName());
                long realDelayed = pubWrapper.getTimestamp() + TimeUnit.SECONDS.toMillis(pubTopic.getDelay()) - System.currentTimeMillis();
                if (realDelayed > 0) {
                    HashedWheelTimer bsTimer = brokerContext.getBsTimer();
                    Timeout timeout = bsTimer.newTimeout(t ->
                                    handle0(pubWrapper, mqttChannel, brokerContext, pubTopic.getName(), messageId, wrapper.getTimestamp())
                                            .then(Mono.fromRunnable(() -> mqttChannel.removeDelayPubTimeout(t)))
                                            .subscribe(),
                            realDelayed, TimeUnit.MILLISECONDS);
                    mqttChannel.addDelayPubTimeout(timeout);
                    //response pub comp, 让publish流程结束
                    return mqttChannel.sendMessage(MqttMessageUtils.createPubComp(messageId), false);
                } else {
                    return handle0(pubWrapper, mqttChannel, brokerContext, pubTopic.getName(), messageId, wrapper.getTimestamp())
                            //最后回复pub comp
                            .then(mqttChannel.sendMessage(MqttMessageUtils.createPubComp(messageId), false));
                }
            }
        } else {
            return mqttChannel.sendMessage(MqttMessageUtils.createPubComp(messageId), false);
        }
    }

    private Mono<Void> handle0(MqttMessageWrapper<MqttPublishMessage> pubWrapper, MqttChannel sender, MqttBrokerContext brokerContext,
                               String topicName, int messageId, long timestamp) {
        MqttPublishMessage qos2Message = pubWrapper.getMessage();
        TopicManager topicManager = brokerContext.getTopicManager();
        MqttMessageStore messageStore = brokerContext.getMessageStore();
        RetryService retryService = brokerContext.getRetryService();

        MqttQoS qos = qos2Message.fixedHeader().qosLevel();
        Set<TopicSubscription> subscriptions = topicManager.getSubscriptions(topicName, qos, sender);
        return Mono.when(subscriptions.stream()
                        //过滤离线会话消息
                        .filter(subscription -> {
                            MqttChannel mqttChannel1 = subscription.getMqttChannel();
                            return filterOfflineSession(mqttChannel1, messageStore,
                                    () -> MqttMessageUtils.wrapPublish(qos2Message, subscription, topicName, mqttChannel1.nextMessageId()), timestamp);
                        })
                        //将消息广播给已订阅的mqtt client
                        .map(subscription -> {
                            MqttChannel mqttChannel1 = subscription.getMqttChannel();
                            return mqttChannel1.sendMessage(MqttMessageUtils.wrapPublish(qos2Message, subscription, topicName, mqttChannel1.nextMessageId()),
                                    subscription.getQoS().value() > 0);
                        })
                        .collect(Collectors.toList()))
                //移除retry task
                .then(Mono.fromRunnable(() -> Optional.ofNullable(retryService.getRetry(sender.generateUuid(MqttMessageType.PUBREC, messageId))).ifPresent(Retry::cancel)));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBREL;
    }
}
