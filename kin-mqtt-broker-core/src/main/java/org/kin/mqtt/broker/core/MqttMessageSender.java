package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.core.cluster.MqttSessionStore;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

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
     * 给指定mqtt client发送mqtt publish message, 如果client离线, 则忽略
     *
     * @param clientId mqtt client id
     * @param message  mqtt message
     * @return complete signal
     */
    public Mono<Void> sendMessageNow(String clientId, MqttPublishMessage message) {
        return broadcastMessageNow(Collections.singleton(clientId), message);
    }

    /**
     * 给指定mqtt client发送mqtt publish message, 如果client离线并session持久化, 则缓存
     *
     * @param clientId mqtt client id
     * @param message  mqtt message
     * @return complete signal
     */
    public Mono<Void> sendMessage(String clientId, MqttPublishMessage message) {
        return broadcastMessage(Collections.singleton(clientId), message);
    }

    /**
     * 给指定多个mqtt client发送mqtt message, 如果client离线, 则忽略
     *
     * @param clientIds mqtt client id列表
     * @param message   mqtt message
     * @return complete signal
     */
    public Mono<Void> broadcastMessageNow(Collection<String> clientIds, MqttPublishMessage message) {
        return broadcastMessage(clientIds, message, false);
    }

    /**
     * 给指定多个mqtt client发送mqtt message, 如果client离线并session持久化, 则缓存
     *
     * @param clientIds mqtt client id列表
     * @param message   mqtt message
     * @return complete signal
     */
    public Mono<Void> broadcastMessage(Collection<String> clientIds, MqttPublishMessage message) {
        return broadcastMessage(clientIds, message, true);
    }

    /**
     * 给指定多个mqtt client发送mqtt message
     *
     * @param clientIds     mqtt client id列表
     * @param message       mqtt message
     * @param saveIfOffline 确定如果client离线是否缓存
     * @return complete signal
     */
    private Mono<Void> broadcastMessage(Collection<String> clientIds, MqttPublishMessage message, boolean saveIfOffline) {
        MqttMessageContext<MqttPublishMessage> messageContext = MqttMessageContext.common(message, brokerContext.getBrokerId(), brokerContext.getBrokerClientId());
        return Flux.fromIterable(clientIds)
                .flatMap(clientId -> {
                    MqttSessionManager sessionManager = brokerContext.getSessionManager();
                    MqttSessionStore sessionStore = brokerContext.getSessionStore();

                    MqttSession mqttSession = sessionManager.get(clientId);
                    if (Objects.nonNull(mqttSession) && mqttSession.isOnline()) {
                        //本client在线, 直接publish
                        MqttFixedHeader fixedHeader = message.fixedHeader();
                        MqttQoS mqttQoS = fixedHeader.qosLevel();
                        //开启retry
                        return mqttSession.sendMessage(message, mqttQoS.value() > 0);
                    } else if (saveIfOffline) {
                        //本client离线, 看看是否在其他broker在线, 否则保存离线消息
                        return sessionStore.get(clientId)
                                .publishOn(brokerContext.getMqttBizScheduler())
                                .map(replica -> {
                                    if (replica.isConnected()) {
                                        brokerContext.getBrokerManager()
                                                .sendMqttMessage(replica.getBrokerId(), clientId, MqttMessageHelper.toReplica(messageContext))
                                                .subscribe();
                                        return true;
                                    }

                                    return false;
                                })
                                .switchIfEmpty(Mono.just(false))
                                .doOnNext(online -> {
                                    if (!online) {
                                        saveOfflineMessage(clientId, messageContext);
                                    }
                                })
                                .then();
                    } else {
                        return Mono.empty();
                    }
                })
                .then();
    }

    /**
     * 保存离线消息
     *
     * @param clientId       接收到的mqtt client id
     * @param messageContext mqtt publish消息上下文
     */
    private void saveOfflineMessage(String clientId, MqttMessageContext<MqttPublishMessage> messageContext) {
        brokerContext.getMessageStore()
                .saveOfflineMessage(clientId, MqttMessageHelper.toReplica(messageContext));
    }
}
