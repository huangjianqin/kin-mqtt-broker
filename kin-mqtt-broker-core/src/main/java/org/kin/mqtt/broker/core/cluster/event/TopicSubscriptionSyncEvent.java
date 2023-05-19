package org.kin.mqtt.broker.core.cluster.event;

import org.kin.mqtt.broker.core.event.InternalMqttEvent;

/**
 * 通知节点同步mqtt broker订阅缓存
 *
 * @author huangjianqin
 * @date 2023/4/25
 */
public class TopicSubscriptionSyncEvent extends AbstractMqttClusterEvent implements InternalMqttEvent {
    private static final long serialVersionUID = 7626595626710074550L;

    /** 单例 */
    public static final TopicSubscriptionSyncEvent INSTANCE = new TopicSubscriptionSyncEvent();

    private TopicSubscriptionSyncEvent() {
    }
}
