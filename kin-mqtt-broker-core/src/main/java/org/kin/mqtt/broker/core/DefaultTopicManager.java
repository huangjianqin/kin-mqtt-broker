package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.core.topic.*;
import org.kin.mqtt.broker.core.topic.share.RandomShareSubLoadBalance;
import org.kin.mqtt.broker.core.topic.share.ShareSubLoadBalance;
import org.kin.mqtt.broker.utils.TopicUtils;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/13
 */
public class DefaultTopicManager implements TopicManager {
    private final TopicFilter simpleFilter = new SimpleTopicFilter();
    private final TopicFilter treeFilter = new TreeTopicFilter();
    /** 共享订阅负载均衡实现 */
    private final ShareSubLoadBalance loadBalance;
    /** 已注册订阅topic(正则表达式) */
    private volatile Set<String> subRegexTopics = new CopyOnWriteArraySet<>();

    public DefaultTopicManager() {
        this(RandomShareSubLoadBalance.INSTANCE);
    }

    public DefaultTopicManager(ShareSubLoadBalance loadBalance) {
        this.loadBalance = loadBalance;
    }

    @Override
    public Set<TopicSubscription> getSubscriptions(String topic, MqttQoS qos, @Nullable MqttSession sender) {
        //普通topic订阅+all(共享topic订阅组内根据策略选一个)
        Set<TopicSubscription> finalSubscriptions = new HashSet<>();

        //寻找匹配topic的所有订阅信息
        Set<TopicSubscription> subscriptions = simpleFilter.getSubscriptions(topic, qos);
        subscriptions.addAll(treeFilter.getSubscriptions(topic, qos));

        //key -> share group, value -> 该组内的订阅信息
        Map<String, List<TopicSubscription>> topic2Subscription = new HashMap<>();
        for (TopicSubscription ts : subscriptions) {
            if (ts.isShare()) {
                //共享topic
                String group = ts.getGroup();
                List<TopicSubscription> groupedSubscriptions = topic2Subscription.get(group);
                if (Objects.isNull(groupedSubscriptions)) {
                    groupedSubscriptions = new ArrayList<>();
                    topic2Subscription.put(group, groupedSubscriptions);
                }
                groupedSubscriptions.add(ts);
            } else {
                //普通topic
                finalSubscriptions.add(ts);
            }
        }

        //共享topic订阅组内根据策略选一个
        for (Map.Entry<String, List<TopicSubscription>> entry : topic2Subscription.entrySet()) {
            List<TopicSubscription> groupedSubscriptions = entry.getValue();

            //配置的负载均衡策略选择一个订阅
            TopicSubscription selected = loadBalance.select(entry.getKey(), groupedSubscriptions);
            finalSubscriptions.add(selected);
        }

        return finalSubscriptions.stream().filter(s -> {
            //如果mqtt client订阅了自己的publish的topic, 将noLocal设置为true, 则mqtt client不会收到该topic自己publish的消息
            return !s.isNoLocal() || !Objects.nonNull(sender) || !s.getMqttSession().equals(sender);
        }).collect(Collectors.toSet());
    }

    @Override
    public void addSubscription(TopicSubscription subscription) {
        String topic = subscription.getTopic();
        if (topic.contains(TopicFilter.SINGLE_LV_SYMBOL) ||
                topic.contains(TopicFilter.MORE_LV_SYMBOL)) {
            treeFilter.addSubscription(subscription);
        } else {
            simpleFilter.addSubscription(subscription);
        }
        subRegexTopics.add(TopicUtils.toRegexTopic(topic));
    }

    @Override
    public void removeSubscription(TopicSubscription subscription) {
        String topic = subscription.getTopic();
        if (topic.contains(TopicFilter.SINGLE_LV_SYMBOL) ||
                topic.contains(TopicFilter.MORE_LV_SYMBOL)) {
            treeFilter.removeSubscription(subscription);
        } else {
            simpleFilter.removeSubscription(subscription);
        }
        subRegexTopics.remove(TopicUtils.toRegexTopic(topic));
    }

    @Override
    public int count() {
        return simpleFilter.count() + treeFilter.count();
    }

    @Override
    public Set<TopicSubscription> getAllSubscriptions() {
        Set<TopicSubscription> subscriptions = simpleFilter.getAllSubscriptions();
        subscriptions.addAll(treeFilter.getAllSubscriptions());
        return subscriptions;
    }

    @Override
    public void removeAllSubscriptions(MqttSession mqttSession) {
        Set<TopicSubscription> subscriptions = mqttSession.getSubscriptions();
        for (TopicSubscription subscription : subscriptions) {
            removeSubscription(subscription);
        }
    }

    @Override
    public void addSubscriptions(Collection<TopicSubscription> subscriptions) {
        for (TopicSubscription subscription : subscriptions) {
            addSubscription(subscription);
        }
    }

    @Override
    public Map<String, Set<MqttSession>> getSubscriptionView() {
        return getAllSubscriptions().stream()
                .collect(Collectors.groupingBy(TopicSubscription::getTopic,
                        Collectors.mapping(TopicSubscription::getMqttSession, Collectors.toSet())));
    }

    @Override
    public Set<String> getAllSubRegexTopics() {
        return new HashSet<>(subRegexTopics);
    }
}
