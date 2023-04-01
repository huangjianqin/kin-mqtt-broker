package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttSession;

/**
 * mqtt client注册成功事件, 即只有非持久化mqtt client上线或者持久化mqtt client第一次上线才会触发
 *
 * @author huangjianqin
 * @date 2022/11/27
 */
public class MqttClientRegisterEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttSession mqttSession;

    public MqttClientRegisterEvent(MqttSession mqttSession) {
        this.mqttSession = mqttSession;
    }

    //getter
    public MqttSession getMqttSession() {
        return mqttSession;
    }
}
