package org.kin.mqtt.broker.core.event;

import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;

/**
 * publish消息事件
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
public class MqttPublishEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttSession mqttSession;
    /** publish消息副本 */
    private final MqttMessageReplica message;

    public MqttPublishEvent(MqttSession mqttSession, MqttMessageReplica message) {
        this.mqttSession = mqttSession;
        this.message = message;
    }

    //getter
    public MqttSession getMqttSession() {
        return mqttSession;
    }

    public MqttMessageReplica getMessage() {
        return message;
    }
}
