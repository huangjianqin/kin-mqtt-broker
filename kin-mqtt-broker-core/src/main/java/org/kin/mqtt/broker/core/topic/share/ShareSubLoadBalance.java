package org.kin.mqtt.broker.core.topic.share;

import org.kin.mqtt.broker.core.topic.TopicSubscription;

import java.util.List;

/**
 * 共享订阅负载均衡实现接口
 *
 * @author huangjianqin
 * @date 2022/12/31
 */
public interface ShareSubLoadBalance {
    /**
     * 基于负载均衡策略选择topic订阅信息
     *
     * @param group              共享订阅组
     * @param topicSubscriptions 同一共享订阅组下所有的topic订阅信息
     * @return 最终选择的topic订阅信息
     */
    TopicSubscription select(String group, List<TopicSubscription> topicSubscriptions);
}
