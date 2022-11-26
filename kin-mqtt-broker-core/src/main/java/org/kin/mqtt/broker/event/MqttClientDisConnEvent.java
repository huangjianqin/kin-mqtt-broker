package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttChannel;

/**
 * mqtt client下线事件
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
public final class MqttClientDisConnEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttChannel mqttChannel;

    public MqttClientDisConnEvent(MqttChannel mqttChannel) {
        this.mqttChannel = mqttChannel;
    }

    //getter
    public MqttChannel getMqttChannel() {
        return mqttChannel;
    }
}
