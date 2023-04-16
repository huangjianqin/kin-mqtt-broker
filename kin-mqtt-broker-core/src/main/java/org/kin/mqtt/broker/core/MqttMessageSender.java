package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

/**
 * 往指定mqtt client发送mqtt message api
 *
 * @author huangjianqin
 * @date 2023/4/16
 */
public class MqttMessageSender {
    /** broker context */
    private final MqttBrokerContext brokerContext;

    public MqttMessageSender(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
    }

    /**
     * 给指定mqtt client发送mqtt message
     *
     * @param clientId mqtt client id
     * @param message  mqtt message
     * @return complete signal
     */
    public Mono<Void> sendMessage(String clientId, MqttMessage message) {
        return broadcastMessage(Collections.singleton(clientId), message);
    }

    /**
     * 给指定多个mqtt client发送mqtt message
     *
     * @param clientIds mqtt client id列表
     * @param message   mqtt message
     * @return complete signal
     */
    public Mono<Void> broadcastMessage(Collection<String> clientIds, MqttMessage message) {
        return Mono.fromCallable(() -> {
                    MqttSessionManager sessionManager = brokerContext.getSessionManager();
                    List<MqttSession> sessions = new ArrayList<>(clientIds.size());
                    for (String clientId : clientIds) {
                        MqttSession mqttSession = sessionManager.get(clientId);
                        if (Objects.isNull(mqttSession) || mqttSession.isOffline()) {
                            continue;
                        }

                        sessions.add(mqttSession);
                    }

                    if (sessions.isEmpty()) {
                        throw new MqttException("can not find any online mqtt session");
                    }

                    return sessions;
                })
                .flatMapMany(Flux::fromIterable)
                //开启retry
                //不纳入Inflight
                .flatMap(session -> {
                    MqttFixedHeader fixedHeader = message.fixedHeader();
                    MqttQoS mqttQoS = fixedHeader.qosLevel();
                    return session.sendMessage(message, mqttQoS.value() > 0, true);
                })
                .then();
    }
}
