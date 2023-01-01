package org.kin.mqtt.broker.core.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.core.MqttChannel;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 * topic管理
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public interface TopicManager extends TopicFilter {
    /**
     * 根据topic匹配已注册的订阅信息
     *
     * @param topic  topic
     * @param qos    publish消息的qos, 用于结合订阅信息并转换为真实订阅qos
     * @param sender 发送publish的mqtt client
     * @return {@link TopicSubscription}
     */
    Set<TopicSubscription> getSubscriptions(String topic, MqttQoS qos, @Nullable MqttChannel sender);

    @Override
    default Set<TopicSubscription> getSubscriptions(String topic, MqttQoS qos) {
        return getSubscriptions(topic, qos, null);
    }


    /**
     * 取消指定mqtt channel的所有订阅
     *
     * @param mqttChannel mqtt channel
     */
    void removeAllSubscriptions(MqttChannel mqttChannel);

    /**
     * 批量注册订阅
     *
     * @param subscriptions 订阅信息
     */
    void addSubscriptions(Set<TopicSubscription> subscriptions);

    /**
     * 获取所有topic信息
     *
     * @return {@link MqttChannel}
     */
    Map<String, Set<MqttChannel>> getSubscriptionView();
}
