package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.Retry;
import org.kin.mqtt.broker.core.RetryService;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.store.MqttMessageStore;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 接收到pub rel消息, 预期响应pub comp消息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
@Extension("pubRel")
public class PubRelHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        MqttMessage message = wrapper.getMessage();
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        int messageId = variableHeader.messageId();
        return mqttChannel.removeQos2Message(messageId)
                .map(qos2Message -> {
                    TopicManager topicManager = brokerContext.getTopicManager();
                    MqttMessageStore messageStore = brokerContext.getMessageStore();
                    RetryService retryService = brokerContext.getRetryService();

                    String topicName = qos2Message.variableHeader().topicName();
                    MqttQoS qos = qos2Message.fixedHeader().qosLevel();
                    Set<TopicSubscription> subscriptions = topicManager.getSubscriptions(topicName, qos);
                    return Mono.when(subscriptions.stream()
                                    //过滤离线会话消息
                                    .filter(subscription -> {
                                        MqttChannel mqttChannel1 = subscription.getMqttChannel();
                                        return filterOfflineSession(mqttChannel1, messageStore,
                                                () -> MqttMessageUtils.wrapPublish(qos2Message, subscription.getQoS(), mqttChannel1.nextMessageId()), wrapper.getTimestamp());
                                    })
                                    //将消息广播给已订阅的mqtt client
                                    .map(subscription -> {
                                        MqttChannel mqttChannel1 = subscription.getMqttChannel();
                                        MqttQoS qoS = subscription.getQoS();
                                        return mqttChannel1.sendMessage(MqttMessageUtils.wrapPublish(qos2Message, qoS, mqttChannel1.nextMessageId()), qoS.value() > 0);
                                    })
                                    .collect(Collectors.toList()))
                            //移除retry task
                            .then(Mono.fromRunnable(() -> Optional.ofNullable(retryService.getRetry(mqttChannel.generateUuid(MqttMessageType.PUBREC, messageId))).ifPresent(Retry::cancel)))
                            //最后回复pub comp
                            .then(mqttChannel.sendMessage(MqttMessageUtils.createPubComp(messageId), false));
                })
                //没有缓存qos2消息, 则直接回复pub comp
                .orElseGet(() -> mqttChannel.sendMessage(MqttMessageUtils.createPubComp(messageId), false));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBREL;
    }
}
