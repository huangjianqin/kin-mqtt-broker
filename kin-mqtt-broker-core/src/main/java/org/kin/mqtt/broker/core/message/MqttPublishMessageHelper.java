package org.kin.mqtt.broker.core.message;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.collection.Tuple;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.topic.PubTopic;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.store.MqttMessageStore;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2023/4/16
 */
public final class MqttPublishMessageHelper {
    private MqttPublishMessageHelper() {
    }

    /**
     * 过滤离线会话消息
     *
     * @param mqttSession  mqtt session
     * @param messageStore mqtt 消息外部存储
     * @param supplier     mqtt publish消息生成逻辑
     * @param timestamp    mqtt消息接收时间戳
     * @return boolean          过滤结果
     */
    public static boolean filterOfflineAndSave(MqttSession mqttSession,
                                               MqttMessageStore messageStore,
                                               MqttMessageContext<MqttPublishMessage> pubMessageContext) {
        if (mqttSession.isOnline()) {
            return true;
        } else {
            //消息外部存储
            messageStore.saveOfflineMessage(MqttMessageHelper.toReplica(pubMessageContext));
            return false;
        }
    }

    /**
     * 广播publish消息
     *
     * @param brokerContext  broker context
     * @param pubTopic       解析publish消息的topic name
     * @param messageContext 接收到的mqtt message context
     * @param sender         mqtt message sender
     * @return complete signal
     */
    public static Mono<Void> broadcastAndSaveIfRetain(MqttBrokerContext brokerContext,
                                                      MqttSession sender,
                                                      PubTopic pubTopic,
                                                      MqttMessageContext<MqttPublishMessage> messageContext) {
        return broadcast(brokerContext, sender, pubTopic, messageContext)
                .then(trySaveRetainMessage(brokerContext.getMessageStore(), messageContext));
    }

    /**
     * 广播publish消息
     *
     * @param brokerContext  broker context
     * @param pubTopic       解析publish消息的topic name
     * @param messageContext 接收到的mqtt message context
     * @return complete signal
     */
    public static Mono<Void> broadcast(MqttBrokerContext brokerContext,
                                       PubTopic pubTopic,
                                       MqttMessageContext<MqttPublishMessage> messageContext) {
        return broadcast(brokerContext, null, pubTopic, messageContext);
    }

    /**
     * 广播publish消息
     *
     * @param brokerContext  broker context
     * @param pubTopic       解析publish消息的topic name
     * @param messageContext 接收到的mqtt message context
     * @param sender         mqtt message sender, 如果broker发布的消息, 则为null
     * @return complete signal
     */
    public static Mono<Void> broadcast(MqttBrokerContext brokerContext,
                                       @Nullable MqttSession sender,
                                       PubTopic pubTopic,
                                       MqttMessageContext<MqttPublishMessage> messageContext) {
        if (messageContext.isExpire()) {
            return Mono.empty();
        }

        int delay = pubTopic.getDelay();
        if (delay > 0) {
            return brokerContext.getDispatcher().handleDelayedPublishMessage(brokerContext, pubTopic, messageContext);
        } else {
            return broadcast0(brokerContext, sender, pubTopic, messageContext);
        }
    }

    /**
     * 广播publish消息
     *
     * @param brokerContext  broker context
     * @param pubTopic       解析publish消息的topic name
     * @param messageContext 接收到的mqtt message context
     * @param sender         mqtt message sender, 如果broker发布的消息, 则为null
     * @return complete signal
     */
    private static Mono<Void> broadcast0(MqttBrokerContext brokerContext,
                                         @Nullable MqttSession sender,
                                         PubTopic pubTopic,
                                         MqttMessageContext<MqttPublishMessage> messageContext) {
        MqttFixedHeader fixedHeader = messageContext.getMessage().fixedHeader();
        MqttQoS qos = fixedHeader.qosLevel();

        Set<TopicSubscription> subscriptions = brokerContext.getTopicManager().getSubscriptions(pubTopic.getName(), qos, sender);

        return Mono.when(subscriptions.stream()
                .map(subscription -> {
                    MqttSession mqttSession = subscription.getMqttSession();
                    return new Tuple<>(subscription,
                            MqttMessageContext.common(messageContext,
                                    MqttMessageHelper.wrapPublish(messageContext.getMessage(), subscription,
                                            pubTopic.getName(), mqttSession.nextMessageId())));
                })
                //过滤离线会话消息
                .filter(t2 -> filterOfflineAndSave(t2.first().getMqttSession(), brokerContext.getMessageStore(), t2.second()))
                //将消息广播给已订阅的mqtt client
                .map(t2 -> {
                    TopicSubscription subscription = t2.first();
                    MqttSession mqttSession = subscription.getMqttSession();
                    return mqttSession.sendMessage(t2.second().getMessage(), subscription.getQoS().value() > 0);
                })
                .collect(Collectors.toList()));
    }

    /**
     * 尝试存储retain消息
     *
     * @param messageStore 外部消息存储
     * @return complete signal
     */
    public static Mono<Void> trySaveRetainMessage(MqttMessageStore messageStore, MqttMessageContext<MqttPublishMessage> messageContext) {
        return Mono.fromRunnable(() -> {
            MqttPublishMessage message = messageContext.getMessage();
            if (message.fixedHeader().isRetain()) {
                //存储retain消息
                messageStore.saveRetainMessage(MqttMessageHelper.toReplica(messageContext));
            }
        });
    }
}
