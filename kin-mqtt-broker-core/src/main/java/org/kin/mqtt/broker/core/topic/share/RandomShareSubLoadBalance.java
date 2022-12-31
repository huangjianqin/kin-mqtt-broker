package org.kin.mqtt.broker.core.topic.share;

import org.kin.mqtt.broker.core.topic.TopicSubscription;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机策略
 *
 * @author huangjianqin
 * @date 2022/12/31
 */
public class RandomShareSubLoadBalance implements ShareSubLoadBalance {
    /** 单例 */
    public static final RandomShareSubLoadBalance INSTANCE = new RandomShareSubLoadBalance();

    @Override
    public TopicSubscription select(String group, List<TopicSubscription> topicSubscriptions) {
        int size = topicSubscriptions.size();
        TopicSubscription selected;
        if (size > 1) {
            selected = topicSubscriptions.get(ThreadLocalRandom.current().nextInt(size));
            if (selected == null) {
                selected = topicSubscriptions.get(0);
            }
        } else {
            selected = topicSubscriptions.get(0);
        }
        return selected;
    }
}
