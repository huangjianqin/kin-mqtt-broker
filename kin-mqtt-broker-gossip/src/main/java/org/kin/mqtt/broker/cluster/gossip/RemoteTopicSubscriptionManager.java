package org.kin.mqtt.broker.cluster.gossip;

import org.kin.framework.utils.CollectionUtils;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * remote broker订阅管理
 *
 * @author huangjianqin
 * @date 2023/4/25
 */
public class RemoteTopicSubscriptionManager {
    /**
     * remote broker订阅信息
     * 单线程处理, 故没有使用同步map
     *
     * @see GossipBrokerManager
     */
    private final Map<String, RemoteTopicSubscription> remoteTopicSubscriptionMap = new HashMap<>();
    /**
     * 该节点已注册订阅topic的正则表达式
     * 存在并发访问
     */
    private final Set<String> subscribedRegexTopics = new CopyOnWriteArraySet<>();

    /**
     * 该broker节点是否有订阅{@code topic}的mqtt client
     *
     * @param topic mqtt topic
     */
    public boolean hasSubscription(String topic) {
        for (String regexTopic : subscribedRegexTopics) {
            if (topic.matches(regexTopic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * remote mqtt broker订阅变化时, 维护该节点的订阅信息
     *
     * @param subscriptions mqtt topic
     */
    public void onSubscriptionChanged(List<RemoteTopicSubscription> changedSubscriptions) {
        //新增订阅topic
        Set<String> subscribeRegexTopics = new HashSet<>();
        //取消订阅topic
        Set<String> unsubscribeRegexTopics = new HashSet<>();
        for (RemoteTopicSubscription changed : changedSubscriptions) {
            String regexTopic = changed.getRegexTopic();
            RemoteTopicSubscription cache = remoteTopicSubscriptionMap.computeIfAbsent(regexTopic, k -> changed);
            if (cache == changed) {
                //之前没有的topic
                subscribeRegexTopics.add(regexTopic);
            } else if (cache.merge(changed.getTid(), changed.isSubscribed())) {
                //exists

                if (cache.isSubscribed()) {
                    subscribeRegexTopics.add(regexTopic);
                } else {
                    unsubscribeRegexTopics.add(regexTopic);
                }
            }
        }

        if (CollectionUtils.isNonEmpty(subscribeRegexTopics)) {
            subscribedRegexTopics.addAll(subscribeRegexTopics);
        }

        if (CollectionUtils.isNonEmpty(unsubscribeRegexTopics)) {
            subscribedRegexTopics.removeAll(unsubscribeRegexTopics);
        }
    }

    @Override
    public String toString() {
        return "RemoteTopicSubscriptionManager{" +
                "remoteTopicSubscriptionMap=" + remoteTopicSubscriptionMap +
                ", subscribedRegexTopics=" + subscribedRegexTopics +
                '}';
    }
}
