package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.core.topic.*;
import org.kin.mqtt.broker.core.topic.share.RandomShareSubLoadBalance;
import org.kin.mqtt.broker.core.topic.share.ShareSubLoadBalance;

import java.util.*;
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

    public DefaultTopicManager() {
        this(RandomShareSubLoadBalance.INSTANCE);
    }

    public DefaultTopicManager(ShareSubLoadBalance loadBalance) {
        this.loadBalance = loadBalance;
    }

    @Override
    public Set<TopicSubscription> getSubscriptions(String topic, MqttQoS qos) {
        //普通主题订阅+all(共享主题订阅组内根据策略选一个)
        Set<TopicSubscription> finalSubscriptions = new HashSet<>();

        //寻找匹配topic的所有订阅信息
        Set<TopicSubscription> subscriptions = simpleFilter.getSubscriptions(topic, qos);
        subscriptions.addAll(treeFilter.getSubscriptions(topic, qos));

        //key -> share group, value -> 该组内的订阅信息
        Map<String, List<TopicSubscription>> topic2Subscription = new HashMap<>();
        for (TopicSubscription ts : subscriptions) {
            if (ts.isShare()) {
                //共享主题
                String group = ts.getGroup();
                List<TopicSubscription> groupedSubscriptions = topic2Subscription.get(group);
                if (Objects.isNull(groupedSubscriptions)) {
                    groupedSubscriptions = new ArrayList<>();
                    topic2Subscription.put(group, groupedSubscriptions);
                }
                groupedSubscriptions.add(ts);
            } else {
                //普通主题
                finalSubscriptions.add(ts);
            }
        }

        //共享主题订阅组内根据策略选一个
        for (Map.Entry<String, List<TopicSubscription>> entry : topic2Subscription.entrySet()) {
            List<TopicSubscription> groupedSubscriptions = entry.getValue();

            //配置的负载均衡策略选择一个订阅
            TopicSubscription selected = loadBalance.select(entry.getKey(), groupedSubscriptions);
            finalSubscriptions.add(selected);
        }

        return finalSubscriptions;
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
    public void removeAllSubscriptions(MqttChannel mqttChannel) {
        Set<TopicSubscription> subscriptions = mqttChannel.getSubscriptions();
        for (TopicSubscription subscription : subscriptions) {
            removeSubscription(subscription);
        }
    }

    @Override
    public void addSubscriptions(Set<TopicSubscription> subscriptions) {
        for (TopicSubscription subscription : subscriptions) {
            addSubscription(subscription);
        }
    }

    @Override
    public Map<String, Set<MqttChannel>> getSubscriptionView() {
        return getAllSubscriptions().stream()
                .collect(Collectors.groupingBy(TopicSubscription::getTopic,
                        Collectors.mapping(TopicSubscription::getMqttChannel, Collectors.toSet())));
    }
}
