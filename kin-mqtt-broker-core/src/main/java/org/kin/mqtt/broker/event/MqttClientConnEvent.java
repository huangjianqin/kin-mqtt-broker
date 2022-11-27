package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttChannel;

/**
 * mqtt client上线事件, 注意持久化mqtt client会触发多次
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
public final class MqttClientConnEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttChannel mqttChannel;

    public MqttClientConnEvent(MqttChannel mqttChannel) {
        this.mqttChannel = mqttChannel;
    }

    //getter
    public MqttChannel getMqttChannel() {
        return mqttChannel;
    }
}
