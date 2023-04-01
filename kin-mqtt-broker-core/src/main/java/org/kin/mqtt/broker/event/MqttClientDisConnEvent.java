package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttSession;

/**
 * mqtt client下线事件, 注意持久化mqtt client会触发多次
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
public class MqttClientDisConnEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttSession mqttSession;

    public MqttClientDisConnEvent(MqttSession mqttSession) {
        this.mqttSession = mqttSession;
    }

    //getter
    public MqttSession getMqttSession() {
        return mqttSession;
    }
}
