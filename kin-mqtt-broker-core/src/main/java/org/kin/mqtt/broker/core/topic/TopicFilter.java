package org.kin.mqtt.broker.core.topic;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.Set;

/**
 * @author huangjianqin
 * @date 2022/11/13
 */
public interface TopicFilter {
    String SEPARATOR = "/";
    /** 单层匹配符 */
    String SINGLE_LV_SYMBOL = "+";
    /** 多层匹配符 */
    String MORE_LV_SYMBOL = "#";

    /**
     * 根据topic匹配已注册的订阅信息
     *
     * @param topic topic
     * @param qos   publish消息的qos, 用于结合订阅信息并转换为真实订阅qos
     * @return {@link TopicSubscription}
     */
    Set<TopicSubscription> getSubscriptions(String topic, MqttQoS qos);

    /**
     * 注册topic订阅
     *
     * @param subscription 订阅信息
     */
    void addSubscription(TopicSubscription subscription);

    /**
     * 移除topic订阅
     *
     * @param subscription 订阅信息
     */
    void removeSubscription(TopicSubscription subscription);

    /**
     * 获取总订阅数
     *
     * @return 总订阅数
     */
    int count();

    /**
     * 获取订所有订阅topic
     *
     * @return 所有订阅信息
     */
    Set<TopicSubscription> getAllSubscriptions();
}
