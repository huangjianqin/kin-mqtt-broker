package org.kin.mqtt.broker;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import reactor.core.Disposable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * mqtt订阅管理
 * @author huangjianqin
 * @date 2022/11/6
 */
final class SubscriptionManager {
    /** 所属mqtt session */
    private final MqttSession session;
    /**
     * key -> topic name, value -> 订阅Disposable
     * 目前仅仅会在netty event loop读写
     */
    private final Map<String, Disposable> topic2SubscriptionDisposable = new HashMap<>();

    SubscriptionManager(MqttSession session) {
        this.session = session;
    }

    /**
     * mqtt client订阅
     * @param channel   netty channel
     * @param topicName topic name
     * @return 是否订阅成功
     */
    boolean subscribe(Channel channel, String topicName) {
        if(topic2SubscriptionDisposable.containsKey(topicName)){
            return false;
        }
        Disposable disposable = TopicMessageStore.INSTANCE.onSubscription(topicName)
                .map(bytes -> createPublishMessage(topicName, bytes))
                .subscribe(channel::writeAndFlush);
        topic2SubscriptionDisposable.put(topicName, disposable);
        return true;
    }

    /**
     * mqtt client取消订阅
     * @param topicName topic name
     */
    void unsubscribe(String topicName) {
        Disposable disposable = topic2SubscriptionDisposable.get(topicName);
        if (Objects.isNull(disposable)) {
            return;
        }

        disposable.dispose();
    }

    /**
     * mqtt client取消所有订阅
     */
    void unsubscribeAll() {
        for (Disposable disposable : topic2SubscriptionDisposable.values()) {
            disposable.dispose();
        }

        topic2SubscriptionDisposable.clear();
    }

    /**
     * 构建订阅响应的publish消息
     * @param topicName topic name
     * @param bytes 响应payload
     * @return  publish消息
     */
    private MqttPublishMessage createPublishMessage(String topicName, ByteBuf bytes) {
        MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, session.nextPackageId());
        return (MqttPublishMessage) MqttMessageFactory.newMessage(header, variableHeader, bytes);
    }
}
