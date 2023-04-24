package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttSession;

import java.util.List;

/**
 * unsubscribe消息事件
 *
 * @author huangjianqin
 * @date 2023/4/24
 */
public class MqttUnsubscribeEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttSession mqttSession;
    /** 取消subscribe的topic列表 */
    private final List<String> topics;

    public MqttUnsubscribeEvent(MqttSession mqttSession, List<String> topics) {
        this.mqttSession = mqttSession;
        this.topics = topics;
    }

    //getter
    public MqttSession getMqttSession() {
        return mqttSession;
    }

    public List<String> getTopics() {
        return topics;
    }
}
