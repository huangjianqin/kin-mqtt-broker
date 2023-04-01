package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.topic.TopicSubscription;

import java.util.Set;

/**
 * subscribe消息事件
 *
 * @author huangjianqin
 * @date 2022/11/26
 */
public class MqttSubscribeEvent implements MqttEvent {
    /** mqtt client信息 */
    private final MqttSession mqttSession;
    /** 该client的订阅 */
    private final Set<TopicSubscription> subscriptions;

    public MqttSubscribeEvent(MqttSession mqttSession, Set<TopicSubscription> subscriptions) {
        this.mqttSession = mqttSession;
        this.subscriptions = subscriptions;
    }

    //getter
    public MqttSession getMqttSession() {
        return mqttSession;
    }

    public Set<TopicSubscription> getSubscriptions() {
        return subscriptions;
    }
}
