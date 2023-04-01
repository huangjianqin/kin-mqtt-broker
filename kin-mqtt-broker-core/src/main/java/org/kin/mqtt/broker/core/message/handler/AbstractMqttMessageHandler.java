package org.kin.mqtt.broker.core.message.handler;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageHandler;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.store.MqttMessageStore;

import java.util.function.Supplier;

/**
 * @author huangjianqin
 * @date 2022/11/14
 */
abstract class AbstractMqttMessageHandler<M extends MqttMessage> implements MqttMessageHandler<M> {
    /**
     * 过滤离线会话消息
     *
     * @param mqttSession  mqtt session
     * @param messageStore mqtt 消息外部存储
     * @param supplier     mqtt publish消息生成逻辑
     * @param timestamp    mqtt消息接收时间戳
     * @return boolean          过滤结果
     */
    protected boolean filterOfflineSession(MqttSession mqttSession, MqttMessageStore messageStore,
                                           Supplier<MqttPublishMessage> supplier, long timestamp) {
        if (mqttSession.isOnline()) {
            return true;
        } else {
            //消息外部存储
            messageStore.saveOfflineMessage(MqttMessageUtils.toReplica(mqttSession.getClientId(), supplier.get(), timestamp));
            return false;
        }
    }
}

