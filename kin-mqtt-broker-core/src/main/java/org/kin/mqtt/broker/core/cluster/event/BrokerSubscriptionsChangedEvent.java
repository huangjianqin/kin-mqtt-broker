package org.kin.mqtt.broker.core.cluster.event;

import org.kin.mqtt.broker.core.event.InternalMqttEvent;

import java.io.Serializable;

/**
 * 通知集群节点同步mqtt broker订阅缓存
 *
 * @author huangjianqin
 * @date 2023/4/25
 */
public class BrokerSubscriptionsChangedEvent extends AbstractMqttClusterEvent implements InternalMqttEvent, Serializable {
    private static final long serialVersionUID = 7626595626710074550L;

    /** mqtt broker id */
    private String brokerId;

    public BrokerSubscriptionsChangedEvent() {
    }

    public BrokerSubscriptionsChangedEvent(String brokerId) {
        this.brokerId = brokerId;
    }

    //setter && getter
    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }
}
