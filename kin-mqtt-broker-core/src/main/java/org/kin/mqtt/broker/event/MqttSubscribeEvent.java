package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.topic.TopicSubscription;

import java.util.Collection;

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
    private final Collection<TopicSubscription> subscriptions;

    public MqttSubscribeEvent(MqttSession mqttSession, Collection<TopicSubscription> subscriptions) {
        this.mqttSession = mqttSession;
        this.subscriptions = subscriptions;
    }

    //getter
    public MqttSession getMqttSession() {
        return mqttSession;
    }

    public Collection<TopicSubscription> getSubscriptions() {
        return subscriptions;
    }
}
