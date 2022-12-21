package org.kin.mqtt.broker.cluster.event;

import org.kin.mqtt.broker.utils.TopicUtils;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * 注册订阅事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public class SubscriptionsAddEvent extends AbstractMqttClusterEvent {
    private static final long serialVersionUID = 8070264180524971433L;

    /** 原始订阅topic */
    private Collection<String> subscriptions;
    /** 订阅topic正则 */
    private Collection<String> subscriptionRegexs;

    public static SubscriptionsAddEvent of(Collection<String> subscriptions) {
        SubscriptionsAddEvent inst = new SubscriptionsAddEvent();
        inst.subscriptions = subscriptions;
        inst.subscriptionRegexs = subscriptions.stream()
                .map(TopicUtils::toRegexTopic)
                .collect(Collectors.toList());
        return inst;
    }

    //setter && getter
    public Collection<String> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(Collection<String> subscriptions) {
        this.subscriptions = subscriptions;
    }

    public Collection<String> getSubscriptionRegexs() {
        return subscriptionRegexs;
    }

    public void setSubscriptionRegexs(Collection<String> subscriptionRegexs) {
        this.subscriptionRegexs = subscriptionRegexs;
    }
}
