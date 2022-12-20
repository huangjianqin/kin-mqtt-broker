package org.kin.mqtt.broker.event;

import org.kin.mqtt.broker.core.MqttChannel;
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
    private final MqttChannel mqttChannel;
    /** 该client的订阅 */
    private final Set<TopicSubscription> subscriptions;

    public MqttSubscribeEvent(MqttChannel mqttChannel, Set<TopicSubscription> subscriptions) {
        this.mqttChannel = mqttChannel;
        this.subscriptions = subscriptions;
    }

    //getter
    public MqttChannel getMqttChannel() {
        return mqttChannel;
    }

    public Set<TopicSubscription> getSubscriptions() {
        return subscriptions;
    }
}
