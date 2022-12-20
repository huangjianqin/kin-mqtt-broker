package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttChannel;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;

/**
 * publish消息事件
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
public class MqttPublishEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttChannel mqttChannel;
    /** publish消息副本 */
    private final MqttMessageReplica message;

    public MqttPublishEvent(MqttChannel mqttChannel, MqttMessageReplica message) {
        this.mqttChannel = mqttChannel;
        this.message = message;
    }

    //getter
    public MqttChannel getMqttChannel() {
        return mqttChannel;
    }

    public MqttMessageReplica getMessage() {
        return message;
    }
}
