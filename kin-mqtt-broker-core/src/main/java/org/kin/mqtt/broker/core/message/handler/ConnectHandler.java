package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.*;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.MqttChannelManager;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.event.MqttClientConnEvent;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/11/15
 */
public class ConnectHandler extends AbstractMqttMessageHandler<MqttConnectMessage> {
    private static final Logger log = LoggerFactory.getLogger(ConnectHandler.class);

    @Override
    public Mono<Void> handle(MqttMessageWrapper<MqttConnectMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        return Mono.from(handle0(wrapper, mqttChannel, brokerContext));
    }

    private Mono<Void> handle0(MqttMessageWrapper<MqttConnectMessage> wrapper, MqttChannel mqttChannel, MqttBrokerContext brokerContext) {
        MqttConnectMessage message = wrapper.getMessage();

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
        return authService.auth(payload.userName(), new String(payload.passwordInBytes(), StandardCharsets.UTF_8))
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
        if (variableHeader.isCleanSession()) {
            //客户端和服务端必须丢弃任何已存在的会话, 并开始一个新的会话
            if (Objects.nonNull(oldMqttChannel)) {
                //持久化session重新上线才会走进这里
                oldMqttChannel.cleanSession(true);
            }
            //mqtt channel设置
            mqttChannel.onConnect(clientId, variableHeader, payload);
        } else {
            if (Objects.nonNull(oldMqttChannel) && !oldMqttChannel.isSessionExpiry()) {
                //存在一个关联此客户端标识符的会话, 服务端必须基于此会话的状态恢复与客户端的通信. 如果不存在任何关联此客户端标识符的会话, 服务端必须创建一个新的会话
                oldMqttChannel.onReconnect(mqttChannel, variableHeader, payload);
                //替换
                mqttChannel = oldMqttChannel;
            } else {
                //mqtt channel设置
                mqttChannel.onConnect(clientId, variableHeader, payload);
            }
        }

        MqttChannel mqttChannelCopy = mqttChannel;
        return mqttChannel.sendMessage(MqttMessageUtils.createConnAck(MqttConnectReturnCode.CONNECTION_ACCEPTED, mqttVersion), false)
                .then(sendOfflineMessage(brokerContext.getMessageStore(), mqttChannel))
                .then(Mono.fromRunnable(() -> brokerContext.broadcastEvent(new MqttClientConnEvent(mqttChannelCopy))));
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
