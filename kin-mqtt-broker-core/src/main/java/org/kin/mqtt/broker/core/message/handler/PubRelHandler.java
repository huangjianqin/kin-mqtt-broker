package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.message.MqttPublishMessageHelper;
import org.kin.mqtt.broker.core.retry.Retry;
import org.kin.mqtt.broker.core.retry.RetryService;
import org.kin.mqtt.broker.core.topic.PubTopic;
import org.kin.mqtt.broker.utils.TopicUtils;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 接收到pub rel消息, 预期响应pub comp消息
 *
 * @author huangjianqin
 * @date 2022/11/14
 */
public class PubRelHandler extends AbstractMqttMessageHandler<MqttMessage> {
    @Override
    public Mono<Void> handle(MqttMessageContext<MqttMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        RetryService retryService = brokerContext.getRetryService();

        MqttMessage message = messageContext.getMessage();
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        int messageId = variableHeader.messageId();
        MqttMessageContext<MqttPublishMessage> pubMessageContext = mqttSession.removeQos2Message(messageId);
        if (Objects.nonNull(pubMessageContext)) {
            Mono<Void> mono;
            if (pubMessageContext.isExpire()) {
                //过期, 回复pub comp
                mono = mqttSession.sendMessage(MqttMessageHelper.createPubComp(messageId), false);
            } else {
                MqttPublishMessage qos2Message = pubMessageContext.getMessage();
                PubTopic pubTopic = TopicUtils.parsePubTopic(qos2Message.variableHeader().topicName());
                long realDelayed = pubMessageContext.getRecTime() + TimeUnit.SECONDS.toMillis(pubTopic.getDelay()) - System.currentTimeMillis();
                if (realDelayed > 0) {
                    mono = brokerContext.getDispatcher().handleDelayedPublishMessage(brokerContext, pubTopic, pubMessageContext)
                            //response pub comp, 让publish流程结束
                            .then(mqttSession.sendMessage(MqttMessageHelper.createPubComp(messageId), false));
                } else {
                    //remove delay info
                    mono = MqttPublishMessageHelper.broadcast(brokerContext, new PubTopic(pubTopic.getName()), pubMessageContext)
                            //最后回复pub comp
                            .then(mqttSession.sendMessage(MqttMessageHelper.createPubComp(messageId), false));
                }
            }

            //移除retry task
            return mono
                    .then(Mono.fromRunnable(() -> Optional.ofNullable(retryService.getRetry(mqttSession.genMqttMessageRetryId(MqttMessageType.PUBREC, messageId))).ifPresent(Retry::cancel)));
        } else {
            return mqttSession.sendMessage(MqttMessageHelper.createPubComp(messageId), false);
        }
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.PUBREL;
    }
}
