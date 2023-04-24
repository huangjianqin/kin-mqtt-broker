package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.event.MqttUnsubscribeEvent;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * @author huangjianqin
 * @date 2022/11/14
 */
public class UnsubscribeHandler extends AbstractMqttMessageHandler<MqttUnsubscribeMessage> {
    @Override
    public Mono<Void> handle(MqttMessageContext<MqttUnsubscribeMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        MqttUnsubscribeMessage message = messageContext.getMessage();
        List<String> topics = message.payload().topics();
        return Mono.fromRunnable(() -> {
                    TopicManager topicManager = brokerContext.getTopicManager();

                    topics.stream()
                            .map(topic -> TopicSubscription.forRemove(topic, mqttSession))
                            .forEach(topicManager::removeSubscription);

                    //持久化session
                    mqttSession.tryPersist();
                }).then(mqttSession.sendMessage(MqttMessageHelper.createUnsubAck(message.variableHeader().messageId()), false))
                //broadcast mqtt event
                .then(Mono.fromRunnable(() -> brokerContext.broadcastEvent(new MqttUnsubscribeEvent(mqttSession, topics))));
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.UNSUBSCRIBE;
    }
}
