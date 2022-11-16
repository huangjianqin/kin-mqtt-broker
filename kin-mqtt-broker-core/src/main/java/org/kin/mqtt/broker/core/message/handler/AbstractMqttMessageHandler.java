package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageHandler;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.store.MqttMessageStore;

import java.util.function.Supplier;

/**
 * @author huangjianqin
 * @date 2022/11/14
 */
abstract class AbstractMqttMessageHandler<M extends MqttMessage> implements MqttMessageHandler<M> {
    /**
     * 过滤离线会话消息
     *
     * @param mqttChannel  mqtt channel
     * @param messageStore mqtt 消息外部存储
     * @param supplier     mqtt publish消息生成逻辑
     * @param timestamp    mqtt消息接收时间戳
     * @return boolean          过滤结果
     */
    protected boolean filterOfflineSession(MqttChannel mqttChannel, MqttMessageStore messageStore,
                                           Supplier<MqttPublishMessage> supplier, long timestamp) {
        if (mqttChannel.isOnline()) {
            return true;
        } else {
            //消息外部存储
            messageStore.saveOfflineMessage(MqttMessageReplica.fromPublishMessage(mqttChannel.getClientId(), supplier.get(), timestamp));
            return false;
        }
    }
}

