package org.kin.mqtt.broker.core.topic;

import org.kin.mqtt.broker.core.MqttChannel;

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
