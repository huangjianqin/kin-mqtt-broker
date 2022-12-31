package org.kin.mqtt.broker.core.topic.share;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.topic.TopicSubscription;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 递增+求余策略
 *
 * @author huangjianqin
 * @date 2022/12/31
 */
public class RoundRobinShareSubLoadBalance implements ShareSubLoadBalance {
    /** 计数器 */
    private final Map<String, AtomicInteger> counters = new NonBlockingHashMap<>();

    @Override
    public TopicSubscription select(String group, List<TopicSubscription> topicSubscriptions) {
        if (CollectionUtils.isEmpty(topicSubscriptions)) {
            return null;
        }
        AtomicInteger counter = counters.computeIfAbsent(group, k -> new AtomicInteger());
        return topicSubscriptions.get(counter.incrementAndGet() % topicSubscriptions.size());
    }
}
