package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.*;
import org.kin.framework.utils.Extension;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.MqttChannelManager;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
@Extension("publish")
public class ConnectHandler extends AbstractMqttMessageHandler<MqttConnectMessage> {
    private static final Logger log = LoggerFactory.getLogger(ConnectHandler.class);

    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttConnectMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        return Mono.from(handle0(wrapper, mqttChannel, brokerContext));
    }

    private Mono<Void> handle0(MqttMessageWrapper<MqttConnectMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        MqttConnectMessage message = wrapper.getMessage();
        // TODO: 2022/11/15
//            MetricManagerHolder.metricManager.getMetricRegistry().getMetricCounter(CounterType.CONNECT_EVENT).increment();
//            EventRegistry eventRegistry = mqttReceiveContext.getEventRegistry();

        MqttConnectVariableHeader variableHeader = message.variableHeader();
        MqttConnectPayload payload = message.payload();
        String clientId = payload.clientIdentifier();

        MqttChannelManager channelManager = brokerContext.getChannelManager();
        AuthService authService = brokerContext.getAuthService();

        byte mqttVersion = (byte) variableHeader.version();

        MqttChannel oldMqttChannel = channelManager.get(clientId);
        if (oldMqttChannel != null && oldMqttChannel.isOnline()) {
            //已登录, 直接reject
            return rejectConnect(mqttChannel, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
        }

        //protocol version support
        if (MqttVersion.MQTT_3_1.protocolLevel() != mqttVersion &&
                MqttVersion.MQTT_3_1_1.protocolLevel() != mqttVersion
                && MqttVersion.MQTT_5.protocolLevel() != mqttVersion) {
            //仅支持3.1, 3.1.1, 5
            return rejectConnect(mqttChannel, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
        }

        //auth
        return authService.auth(payload.userName(), payload.passwordInBytes(), clientId)
                .flatMap(authResult -> {
                    if (authResult) {
                        return handle1(oldMqttChannel, mqttChannel, brokerContext, variableHeader, payload, clientId, mqttVersion);
                    } else {
                        return rejectConnect(mqttChannel, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                    }
                });
    }

    private Mono<Void> handle1(MqttChannel oldMqttChannel, MqttChannel mqttChannel, MqttBrokerContext brokerContext,
                               MqttConnectVariableHeader variableHeader, MqttConnectPayload payload,
                               String clientId, byte mqttVersion) {
        TopicManager topicManager = brokerContext.getTopicManager();
        MqttChannelManager channelManager = brokerContext.getChannelManager();

        //old channel处理
        if (Objects.nonNull(oldMqttChannel)) {
            Set<TopicSubscription> subscriptions = oldMqttChannel.getSubscriptions()
                    .stream()
                    .map(subscription -> new TopicSubscription(subscription.getTopic(), subscription.getQoS(), mqttChannel))
                    .collect(Collectors.toSet());
            // remove old channel
            channelManager.remove(clientId);
            topicManager.removeAllSubscriptions(oldMqttChannel);
        }

        //连接成功后, mqtt channel设置
        mqttChannel.onConnectSuccess(clientId, variableHeader, payload);

        return mqttChannel.sendMessage(MqttMessageUtils.createConnAck(MqttConnectReturnCode.CONNECTION_ACCEPTED, mqttVersion), false)
                .then(sendOfflineMessage(brokerContext.getMessageStore(), mqttChannel));
    }


    /**
     * 拒绝连接统一入口
     *
     * @param mqttChannel mqtt channel
     * @param mqttVersion mqtt版本
     * @return complete signal
     */
    private Mono<Void> rejectConnect(MqttChannel mqttChannel, byte mqttVersion, MqttConnectReturnCode returnCode) {
        return mqttChannel.sendMessage(MqttMessageUtils.createConnAck(returnCode, mqttVersion), false).then(mqttChannel.close());
    }

    /**
     * 发送该mqtt client离线时接收到的mqtt消息
     *
     * @param messageStore 外部mqtt消息存储
     * @param mqttChannel  mqtt client
     */
    private Mono<Void> sendOfflineMessage(MqttMessageStore messageStore, MqttChannel mqttChannel) {
        return messageStore.getOfflineMessage(mqttChannel.getClientId())
                //以往异常导致正常流程无法继续
                .onErrorResume(t -> {
                    log.error("", t);
                    return Flux.empty();
                })
                .flatMap(replica -> mqttChannel.sendMessage(MqttMessageUtils.createPublish(mqttChannel, replica), replica.getQos() > 0))
                .then();
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.CONNECT;
    }
}
