package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.*;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.Retry;
import org.kin.mqtt.broker.core.RetryService;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
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
    public Mono<Void> handle(MqttMessageContext<MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        MqttMessage message = messageContext.getMessage();
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        int messageId = variableHeader.messageId();
        MqttMessageContext<MqttPublishMessage> pubMessageContext = mqttSession.removeQos2Message(messageId);
        if (Objects.nonNull(pubMessageContext)) {
            if (pubMessageContext.isExpire()) {
                //过期, 回复pub comp
                return mqttSession.sendMessage(MqttMessageUtils.createPubComp(messageId), false);
            } else {
                MqttPublishMessage qos2Message = pubMessageContext.getMessage();
                PubTopic pubTopic = TopicUtils.parsePubTopic(qos2Message.variableHeader().topicName());
                long realDelayed = pubMessageContext.getTimestamp() + TimeUnit.SECONDS.toMillis(pubTopic.getDelay()) - System.currentTimeMillis();
                if (realDelayed > 0) {
                    HashedWheelTimer bsTimer = brokerContext.getBsTimer();
                    Timeout timeout = bsTimer.newTimeout(t ->
                                    handle0(pubMessageContext, mqttSession, brokerContext, pubTopic.getName(), messageId, messageContext.getTimestamp())
                                            .then(Mono.fromRunnable(() -> mqttSession.removeDelayPubTimeout(t)))
                                            .subscribe(),
                            realDelayed, TimeUnit.MILLISECONDS);
                    mqttSession.addDelayPubTimeout(timeout);
                    //response pub comp, 让publish流程结束
                    return mqttSession.sendMessage(MqttMessageUtils.createPubComp(messageId), false);
                } else {
                    return handle0(pubMessageContext, mqttSession, brokerContext, pubTopic.getName(), messageId, messageContext.getTimestamp())
                            //最后回复pub comp
                            .then(mqttSession.sendMessage(MqttMessageUtils.createPubComp(messageId), false));
                }
            }
        } else {
            return mqttSession.sendMessage(MqttMessageUtils.createPubComp(messageId), false);
        }
    }

    private Mono<Void> handle0(MqttMessageContext<MqttPublishMessage> pubMessageContext, MqttSession sender, MqttBrokerContext brokerContext,
                               String topicName, int messageId, long timestamp) {
        MqttPublishMessage qos2Message = pubMessageContext.getMessage();
        TopicManager topicManager = brokerContext.getTopicManager();
        MqttMessageStore messageStore = brokerContext.getMessageStore();
        RetryService retryService = brokerContext.getRetryService();

        MqttQoS qos = qos2Message.fixedHeader().qosLevel();
        Set<TopicSubscription> subscriptions = topicManager.getSubscriptions(topicName, qos, sender);
        return Mono.when(subscriptions.stream()
                        //过滤离线会话消息
                        .filter(subscription -> {
                            MqttSession mqttSession1 = subscription.getMqttSession();
                            return filterOfflineSession(mqttSession1, messageStore,
                                    () -> MqttMessageUtils.wrapPublish(qos2Message, subscription, topicName, mqttSession1.nextMessageId()), timestamp);
                        })
                        //将消息广播给已订阅的mqtt client
                        .map(subscription -> {
                            MqttSession mqttSession1 = subscription.getMqttSession();
                            return mqttSession1.sendMessage(MqttMessageUtils.wrapPublish(qos2Message, subscription, topicName, mqttSession1.nextMessageId()),
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
