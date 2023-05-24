package org.kin.mqtt.broker.core;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.core.cluster.MqttSessionStore;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ReactorNetty;

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
     * @param topic    mqtt topic
     * @param byteBuf  mqtt message content
     * @return complete signal
     */
    public Mono<Void> sendMessageNow(String clientId, String topic, ByteBuf byteBuf) {
        return broadcastMessageNow(Collections.singleton(clientId), topic, MqttQoS.AT_MOST_ONCE, byteBuf);
    }

    /**
     * 给指定mqtt client发送mqtt publish message, 如果client离线, 则忽略
     *
     * @param clientId mqtt client id
     * @param topic    mqtt topic
     * @param qos      mqtt qos
     * @param byteBuf  mqtt message content
     * @return complete signal
     */
    public Mono<Void> sendMessageNow(String clientId, String topic, MqttQoS qos, ByteBuf byteBuf) {
        return broadcastMessageNow(Collections.singleton(clientId), topic, qos, byteBuf);
    }

    /**
     * 给指定mqtt client发送mqtt publish message, 如果client离线并session持久化, 则缓存
     *
     * @param clientId mqtt client id
     * @param topic    mqtt topic
     * @param byteBuf  mqtt message content
     * @return complete signal
     */
    public Mono<Void> sendMessage(String clientId, String topic, ByteBuf byteBuf) {
        return broadcastMessage(Collections.singleton(clientId), topic, MqttQoS.AT_MOST_ONCE, byteBuf);
    }

    /**
     * 给指定mqtt client发送mqtt publish message, 如果client离线并session持久化, 则缓存
     *
     * @param clientId mqtt client id
     * @param topic    mqtt topic
     * @param qos      mqtt qos
     * @param byteBuf  mqtt message content
     * @return complete signal
     */
    public Mono<Void> sendMessage(String clientId, String topic, MqttQoS qos, ByteBuf byteBuf) {
        return broadcastMessage(Collections.singleton(clientId), topic, qos, byteBuf);
    }

    /**
     * 给指定多个mqtt client发送mqtt message, 如果client离线, 则忽略
     *
     * @param clientIds mqtt client id列表
     * @param topic    mqtt topic
     * @param qos      mqtt qos
     * @param byteBuf  mqtt message content
     * @return complete signal
     */
    public Mono<Void> broadcastMessageNow(Collection<String> clientIds, String topic, MqttQoS qos, ByteBuf byteBuf) {
        return broadcastMessage(clientIds, topic, qos, byteBuf, false);
    }

    /**
     * 给指定多个mqtt client发送mqtt message, 如果client离线并session持久化, 则缓存
     *
     * @param clientIds mqtt client id列表
     * @param topic    mqtt topic
     * @param qos      mqtt qos
     * @param byteBuf  mqtt message content
     * @return complete signal
     */
    public Mono<Void> broadcastMessage(Collection<String> clientIds, String topic, MqttQoS qos, ByteBuf byteBuf) {
        return broadcastMessage(clientIds, topic, qos, byteBuf, true);
    }

    /**
     * 给指定多个mqtt client发送mqtt message
     *
     * @param clientIds     mqtt client id列表
     * @param topic    mqtt topic
     * @param qos      mqtt qos
     * @param byteBuf  mqtt message content
     * @param saveIfOffline 确定如果client离线是否缓存
     * @return complete signal
     */
    private Mono<Void> broadcastMessage(Collection<String> clientIds, String topic, MqttQoS qos, ByteBuf byteBuf, boolean saveIfOffline) {
        return Flux.fromIterable(clientIds)
                .flatMap(clientId -> {
                    MqttSessionManager sessionManager = brokerContext.getSessionManager();
                    MqttSessionStore sessionStore = brokerContext.getSessionStore();

                    MqttSession mqttSession = sessionManager.get(clientId);
                    if (Objects.nonNull(mqttSession) && mqttSession.isOnline()) {
                        MqttPublishMessage message = MqttMessageHelper.createPublish(false, qos, mqttSession.nextMessageId(), topic, byteBuf.copy());
                        //本client在线, 直接publish
                        MqttFixedHeader fixedHeader = message.fixedHeader();
                        MqttQoS mqttQoS = fixedHeader.qosLevel();
                        //开启retry
                        return mqttSession.sendMessage(message, mqttQoS.value() > 0);
                    } else if (saveIfOffline) {
                        MqttPublishMessage message = MqttMessageHelper.createPublish(false, qos, 0, topic, byteBuf);
                        MqttMessageContext<MqttPublishMessage> messageContext = MqttMessageContext.common(message, brokerContext.getBrokerId(), brokerContext.getBrokerClientId());
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
                .then()
                .doOnSuccess(v -> ReactorNetty.safeRelease(byteBuf));
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
