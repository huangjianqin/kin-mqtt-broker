package org.kin.mqtt.broker.core.topic;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 复杂的topic匹配, 支持(+#)通配符
 * 节点的新增和删除会对root节点加锁情况下进行操作, 遍历则不需要加锁. 如果不考虑取消订阅, 无用节点的残留问题(本质上算是内存泄漏), 可以全程无锁
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public final class TreeTopicFilter implements TopicFilter {
    /** 根节点 */
    private final TreeNode root = new TreeNode("$ROOT$");
    /** 订阅数统计 */
    private final LongAdder counter = new LongAdder();

    @Override
    public Set<TopicSubscription> getSubscriptions(String topic, MqttQoS qos) {
        return root.getSubscriptions(topic).stream().map(s -> s.convert(qos)).collect(Collectors.toSet());
    }

    @Override
    public void addSubscription(TopicSubscription subscription) {
        boolean addResult;
        synchronized (root) {
            addResult = root.addSubscription(subscription);
        }
        if (addResult) {
            counter.increment();
            subscription.onLinked();
        }
    }

    @Override
    public void removeSubscription(TopicSubscription subscription) {
        boolean removeResult;
        synchronized (root) {
            removeResult = root.removeSubscription(subscription);
        }
        if (removeResult) {
            counter.decrement();
            subscription.onUnlinked();
        }
    }

    @Override
    public int count() {
        return counter.intValue();
    }

    @Override
    public Set<TopicSubscription> getAllSubscriptions() {
        return root.getAllSubscriptions();
    }
}
