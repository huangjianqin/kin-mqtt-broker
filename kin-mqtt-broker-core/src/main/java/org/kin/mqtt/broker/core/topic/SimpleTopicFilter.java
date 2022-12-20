package org.kin.mqtt.broker.core.topic;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.framework.utils.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 简单的topic匹配, 即直接topic name相同就算满足条件
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public class SimpleTopicFilter implements TopicFilter {
    /** key -> topic name, value -> */
    private final Map<String, CopyOnWriteArraySet<TopicSubscription>> topic2Subscriptions = new ConcurrentHashMap<>();
    /** 订阅数统计 */
    private final LongAdder counter = new LongAdder();

    @Override
    public Set<TopicSubscription> getSubscriptions(String topic, MqttQoS qoS) {
        CopyOnWriteArraySet<TopicSubscription> subscriptions = topic2Subscriptions.get(topic);
        if (CollectionUtils.isEmpty(subscriptions)) {
            return Collections.emptySet();
        }
        return subscriptions.stream().map(s -> s.convert(qoS)).collect(Collectors.toSet());
    }

    @Override
    public void addSubscription(TopicSubscription subscription) {
        CopyOnWriteArraySet<TopicSubscription> subscriptions = topic2Subscriptions.computeIfAbsent(subscription.getTopic(), t -> new CopyOnWriteArraySet<>());
        if (subscriptions.add(subscription)) {
            counter.increment();
            subscription.onLinked();
        }
    }

    @Override
    public void removeSubscription(TopicSubscription subscription) {
        CopyOnWriteArraySet<TopicSubscription> subscriptions = topic2Subscriptions.computeIfAbsent(subscription.getTopic(), t -> new CopyOnWriteArraySet<>());
        if (subscriptions.remove(subscription)) {
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
        return topic2Subscriptions.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }
}
