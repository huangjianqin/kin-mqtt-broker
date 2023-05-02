package org.kin.mqtt.broker.core.message.handler;

import io.micrometer.core.instrument.Metrics;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.acl.AclAction;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.message.MqttPublishMessageHelper;
import org.kin.mqtt.broker.core.topic.PubTopic;
import org.kin.mqtt.broker.metrics.MetricsNames;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.mqtt.broker.utils.TopicUtils;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
public class PublishHandler extends AbstractMqttMessageHandler<MqttPublishMessage> {
    @Override
    public Mono<Void> handle(MqttMessageContext<MqttPublishMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        //单个连接消息速率整型
        mqttSession.checkPubMessageRate();
        Metrics.counter(MetricsNames.PUBLISH_MSG_COUNT).increment();

        //acl访问权限检查
        AclService aclService = brokerContext.getAclService();
        MqttPublishMessage message = messageContext.getMessage();
        MqttPublishVariableHeader variableHeader = message.variableHeader();
        String topicName = variableHeader.topicName();
        return aclService.checkPermission(mqttSession.getHost(), mqttSession.getClientId(), mqttSession.getUserName(), topicName, AclAction.PUBLISH)
                .flatMap(aclResult -> {
                    if (aclResult) {
                        //允许访问
                        return handle0(messageContext, mqttSession, brokerContext);
                    } else {
                        return Mono.error(new IllegalStateException(String.format("mqtt publish message for topic '%s' acl is not allowed, %s", topicName, message)));
                    }
                });
    }

    private Mono<Void> handle0(MqttMessageContext<MqttPublishMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        MqttMessageStore messageStore = brokerContext.getMessageStore();

        MqttPublishMessage message = messageContext.getMessage();
        MqttPublishVariableHeader variableHeader = message.variableHeader();
        int packetId = variableHeader.packetId();
        MqttQoS qos = message.fixedHeader().qosLevel();

        PubTopic pubTopic = TopicUtils.parsePubTopic(variableHeader.topicName());

        switch (qos) {
            case AT_MOST_ONCE:
                return MqttPublishMessageHelper.broadcastAndSaveIfRetain(brokerContext, pubTopic, messageContext);
            case AT_LEAST_ONCE:
                if (mqttSession.tryEnqueueInflightQueue(messageContext)) {
                    //进入了inflight queue等待
                    return Mono.empty();
                }

                return MqttPublishMessageHelper.broadcast(brokerContext, pubTopic, messageContext)
                        .then(mqttSession.sendMessage(MqttMessageHelper.createPubAck(packetId), false))
                        .then(MqttPublishMessageHelper.trySaveRetainMessage(messageStore, messageContext));
            case EXACTLY_ONCE:
                if (!mqttSession.existQos2Message(packetId)) {
                    if (mqttSession.tryEnqueueInflightQueue(messageContext)) {
                        //进入了inflight queue等待
                        return Mono.empty();
                    }

                    //mqtt publisher     ->     broker: publish
                    //mqtt publisher     <-     broker: pub rec
                    //mqtt publisher     ->     broker: pub rel
                    //mqtt subscriber... <-     broker: publish
                    //mqtt publisher     <-     broker: pub comp
                    return mqttSession
                            .cacheQos2Message(packetId,
                                    //暂不移除topic中delayed相关信息, pub rel时再移除
                                    MqttMessageContext.common(messageContext, MqttMessageHelper.wrapPublish(message, qos, 0)))
                            .then(mqttSession.sendMessage(MqttMessageHelper.createPubRec(packetId), true));
                }
            default:
                return Mono.empty();
        }
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBLISH;
    }
}
