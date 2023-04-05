package org.kin.mqtt.broker.core.message;

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.Timeout;

import java.util.Objects;

/**
 * 缓存的exactly once mqtt message
 *
 * @author huangjianqin
 * @date 2023/1/1
 */
public class MqttQos2PubMessage {
    /** mqtt publish message context */
    private final MqttMessageContext<MqttPublishMessage> messageContext;
    /** qos2 publish message expire task, 可能为null, 即永不过期 */
    private final Timeout expireTimeout;

    public MqttQos2PubMessage(MqttMessageContext<MqttPublishMessage> messageContext, Timeout expireTimeout) {
        this.messageContext = messageContext;
        this.expireTimeout = expireTimeout;
    }

    /**
     * 取消qos2 publish message expire task
     */
    public void cancelExpireTimeout() {
        if (Objects.isNull(expireTimeout)) {
            return;
        }

        expireTimeout.cancel();
    }

    //getter
    public MqttMessageContext<MqttPublishMessage> getMessageContext() {
        return messageContext;
    }
}
