package org.kin.mqtt.broker.core;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.kin.mqtt.broker.core.topic.*;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/13
 */
public final class DefaultTopicManager implements TopicManager {
    private final TopicFilter simpleFilter = new SimpleTopicFilter();
    private final TopicFilter treeFilter = new TreeTopicFilter();

    @Override
    public Set<TopicSubscription> getSubscriptions(String topic, MqttQoS qos) {
        Set<TopicSubscription> subscriptions = simpleFilter.getSubscriptions(topic, qos);
        subscriptions.addAll(treeFilter.getSubscriptions(topic, qos));
        return subscriptions;
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
