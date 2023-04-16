package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.*;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.MqttSessionManager;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
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
    public Mono<Void> handle(MqttMessageContext<MqttConnectMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        return Mono.from(handle0(messageContext, mqttSession, brokerContext));
    }

    private Mono<Void> handle0(MqttMessageContext<MqttConnectMessage> messageContext, MqttSession mqttSession, MqttBrokerContext brokerContext) {
        MqttConnectMessage message = messageContext.getMessage();

        MqttConnectVariableHeader variableHeader = message.variableHeader();
        MqttConnectPayload payload = message.payload();
        String clientId = payload.clientIdentifier();

        MqttSessionManager sessionManager = brokerContext.getSessionManager();
        AuthService authService = brokerContext.getAuthService();

        byte mqttVersion = (byte) variableHeader.version();

        MqttSession oldMqttSession = sessionManager.get(clientId);
        if (oldMqttSession != null && oldMqttSession.isOnline()) {
            //已登录, 直接reject
            return rejectConnect(mqttSession, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, true);
        }

        //protocol version support
        if (MqttVersion.MQTT_3_1.protocolLevel() != mqttVersion &&
                MqttVersion.MQTT_3_1_1.protocolLevel() != mqttVersion
                && MqttVersion.MQTT_5.protocolLevel() != mqttVersion) {
            //仅支持3.1, 3.1.1, 5
            return rejectConnect(mqttSession, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false);
        }

        //auth
        return authService.auth(payload.userName(), new String(payload.passwordInBytes(), StandardCharsets.UTF_8))
                .flatMap(authResult -> {
                    if (authResult) {
                        return handle1(oldMqttSession, mqttSession, brokerContext, variableHeader, payload, clientId, mqttVersion);
                    } else {
                        return rejectConnect(mqttSession, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false);
                    }
                });
    }

    private Mono<Void> handle1(MqttSession oldMqttSession, MqttSession mqttSession, MqttBrokerContext brokerContext,
                               MqttConnectVariableHeader variableHeader, MqttConnectPayload payload,
                               String clientId, byte mqttVersion) {
        boolean sessionPresent = false;
        if (variableHeader.isCleanSession()) {
            //客户端和服务端必须丢弃任何已存在的会话, 并开始一个新的会话
            if (Objects.nonNull(oldMqttSession)) {
                //持久化session重新上线才会走进这里
                oldMqttSession.cleanSession(true);
            }
            //更新mqtt session属性
            mqttSession.onConnect(clientId, variableHeader, payload);
        } else {
            if (Objects.nonNull(oldMqttSession) && !oldMqttSession.isSessionExpiry()) {
                //存在一个关联此客户端标识符的会话, 服务端必须基于此会话的状态恢复与客户端的通信. 如果不存在任何关联此客户端标识符的会话, 服务端必须创建一个新的会话
                oldMqttSession.onReconnect(mqttSession, variableHeader, payload);
                //替换
                mqttSession = oldMqttSession;
                sessionPresent = true;
            } else {
                //更新mqtt session属性
                mqttSession.onConnect(clientId, variableHeader, payload);
            }
        }

        MqttSession finalMqttSession = mqttSession;
        return mqttSession.sendMessage(MqttMessageHelper.createConnAck(MqttConnectReturnCode.CONNECTION_ACCEPTED, mqttVersion, sessionPresent, brokerContext.getBrokerConfig()), false)
                .then(sendOfflineMessage(brokerContext.getMessageStore(), mqttSession))
                .then(Mono.fromRunnable(() -> brokerContext.broadcastEvent(new MqttClientConnEvent(finalMqttSession))));
    }


    /**
     * 拒绝连接统一入口
     *
     * @param mqttSession mqtt session
     * @param mqttVersion mqtt版本
     * @return complete signal
     */
    private Mono<Void> rejectConnect(MqttSession mqttSession, byte mqttVersion, MqttConnectReturnCode returnCode, boolean sessionPresent) {
        return mqttSession.sendMessage(MqttMessageHelper.createConnAck(returnCode, mqttVersion, sessionPresent), false).then(mqttSession.close());
    }

    /**
     * 发送该mqtt client离线时接收到的mqtt消息
     *
     * @param messageStore 外部mqtt消息存储
     * @param mqttSession  mqtt client
     */
    private Mono<Void> sendOfflineMessage(MqttMessageStore messageStore, MqttSession mqttSession) {
        return messageStore.getOfflineMessage(mqttSession.getClientId())
                //以往异常导致正常流程无法继续
                .onErrorResume(t -> {
                    log.error("", t);
                    return Flux.empty();
                })
                .flatMap(replica -> mqttSession.sendMessage(MqttMessageHelper.createPublish(mqttSession, replica), replica.getQos() > 0))
                .then();
    }

    @Nonnull
    @Override
    public MqttMessageType getMqttMessageType() {
        return MqttMessageType.CONNECT;
    }
}
