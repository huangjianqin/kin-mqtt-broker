package org.kin.mqtt.broker.cluster.gossip.event;

import org.kin.mqtt.broker.cluster.event.AbstractMqttClusterEvent;
import org.kin.mqtt.broker.cluster.gossip.RemoteTopicSubscription;
import org.kin.mqtt.broker.event.InternalMqttEvent;

import java.util.Collections;
import java.util.List;

/**
 * gossip节点间同步mqtt broker订阅事件
 *
 * @author huangjianqin
 * @date 2023/4/25
 */
public class TopicSubscriptionSyncEvent extends AbstractMqttClusterEvent implements InternalMqttEvent {
    private static final long serialVersionUID = 7626595626710074550L;

    /** 变化的订阅信息 */
    private List<RemoteTopicSubscription> changedSubscriptions = Collections.emptyList();

    public TopicSubscriptionSyncEvent() {
    }

    public TopicSubscriptionSyncEvent(List<RemoteTopicSubscription> changedSubscriptions) {
        this.changedSubscriptions = changedSubscriptions;
    }

    public TopicSubscriptionSyncEvent(String address, List<RemoteTopicSubscription> changedSubscriptions) {
        super.address = address;
        this.changedSubscriptions = changedSubscriptions;
    }

    //setter && getter
    public List<RemoteTopicSubscription> getChangedSubscriptions() {
        return changedSubscriptions;
    }

    public void setChangedSubscriptions(List<RemoteTopicSubscription> changedSubscriptions) {
        this.changedSubscriptions = changedSubscriptions;
    }
}
