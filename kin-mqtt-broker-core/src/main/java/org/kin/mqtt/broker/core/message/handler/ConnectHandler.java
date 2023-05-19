package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.*;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.MqttSessionManager;
import org.kin.mqtt.broker.core.event.MqttClientConnEvent;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.session.MqttSessionReplica;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.mqtt.broker.store.MqttSessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
        // TODO: 2023/4/23 MqttConnectReturnCode#CONNECTION_REFUSED_CONNECTION_RATE_EXCEEDED 是否此处拦截connection rate更合适
        MqttConnectMessage message = messageContext.getMessage();

        MqttConnectVariableHeader variableHeader = message.variableHeader();
        MqttConnectPayload payload = message.payload();
        String clientId = payload.clientIdentifier();

        MqttSessionManager sessionManager = brokerContext.getSessionManager();

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

        MqttSessionStore sessionStore = brokerContext.getSessionStore();
        return sessionStore
                .get(clientId)
                .publishOn(brokerContext.getMqttBizScheduler())
                //存在持久化session
                .flatMap(sessionReplica -> {
                    if (!brokerContext.getBrokerId().equals(sessionReplica.getBrokerId()) &&
                            sessionReplica.isConnected()) {
                        //mqtt client已经连接到其他broker, 相当于已登录
                        return rejectConnect(mqttSession, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, true)
                                .then(mqttSession.close())
                                .thenReturn(false);
                    } else {
                        return connect(messageContext, mqttSession, brokerContext, sessionReplica)
                                .thenReturn(true);
                    }
                })
                .onErrorResume(e -> {
                    log.error("get session(clientId={}) from {} error", clientId, sessionStore.getClass().getSimpleName(), e);
                    return rejectConnect(mqttSession, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR, false)
                            .then(mqttSession.close())
                            .thenReturn(false);
                })
                //合法性校验通过 or 不存在持久化session or 异常
                .switchIfEmpty(connect(messageContext, mqttSession, brokerContext, null)
                        .thenReturn(true))
                .then();
    }

    /**
     * 连接逻辑处理
     */
    private Mono<Void> connect(MqttMessageContext<MqttConnectMessage> messageContext,
                               MqttSession mqttSession,
                               MqttBrokerContext brokerContext,
                               @Nullable MqttSessionReplica sessionReplica) {
        MqttConnectMessage message = messageContext.getMessage();

        MqttConnectVariableHeader variableHeader = message.variableHeader();
        MqttConnectPayload payload = message.payload();
        String clientId = payload.clientIdentifier();

        AuthService authService = brokerContext.getAuthService();

        byte mqttVersion = (byte) variableHeader.version();

        //auth
        return authService.auth(payload.userName(), new String(payload.passwordInBytes(), StandardCharsets.UTF_8))
                .flatMap(authResult -> {
                    if (authResult) {
                        return initSession(mqttSession, variableHeader, payload, clientId, sessionReplica)
                                //resp connect ack
                                .flatMap(s -> s.sendMessage(MqttMessageHelper.createConnAck(MqttConnectReturnCode.CONNECTION_ACCEPTED,
                                                mqttVersion, s.isSessionPresent(), brokerContext.getBrokerConfig()), false)
                                        .then(sendOfflineMessage(brokerContext.getMessageStore(), mqttSession))
                                        .then(Mono.fromRunnable(() -> brokerContext.broadcastEvent(new MqttClientConnEvent(s)))));
                    } else {
                        return rejectConnect(mqttSession, mqttVersion, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false);
                    }
                });
    }

    /**
     * 初始化mqtt session
     */
    private Mono<MqttSession> initSession(MqttSession mqttSession,
                                          MqttConnectVariableHeader variableHeader,
                                          MqttConnectPayload payload,
                                          String clientId,
                                          @Nullable MqttSessionReplica sessionReplica) {
        //持久化session是否有效
        boolean sessionReplicaValid = Objects.nonNull(sessionReplica) && sessionReplica.isValid();
        Mono<MqttSession> sessionMono;
        if (!variableHeader.isCleanSession() && sessionReplicaValid) {
            //存在一个关联此客户端标识符的会话, 服务端必须基于此会话的状态恢复与客户端的通信. 如果不存在任何关联此客户端标识符的会话, 服务端必须创建一个新的会话
            sessionMono = Mono.just(mqttSession.onConnect(clientId, variableHeader, payload, sessionReplica));
        } else {
            //客户端和服务端必须丢弃任何已存在的会话, 并开始一个新的会话
            //持久化session已过期
            sessionMono = Mono.just(mqttSession.onConnect(clientId, variableHeader, payload));
        }

        return sessionMono
                //持久化
                .doOnNext(MqttSession::tryPersist);
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
        return messageStore.getAndRemoveOfflineMessage(mqttSession.getClientId())
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
