package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttSession;

/**
 * mqtt client注销成功事件, 即只有非持久化mqtt client下线才会触发
 *
 * @author huangjianqin
 * @date 2022/11/27
 */
public class MqttClientUnregisterEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttSession mqttSession;

    public MqttClientUnregisterEvent(MqttSession mqttSession) {
        this.mqttSession = mqttSession;
    }

    //getter
    public MqttSession getMqttSession() {
        return mqttSession;
    }
}
