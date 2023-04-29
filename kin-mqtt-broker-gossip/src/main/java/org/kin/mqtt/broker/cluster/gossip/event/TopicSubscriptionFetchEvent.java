package org.kin.mqtt.broker.cluster.gossip.event;

import org.kin.mqtt.broker.cluster.event.MqttClusterEvent;
import org.kin.mqtt.broker.event.InternalMqttEvent;

/**
 * 向指定节点fetch订阅信息事件
 *
 * @author huangjianqin
 * @date 2023/4/29
 */
public class TopicSubscriptionFetchEvent implements MqttClusterEvent, InternalMqttEvent {
    private static final long serialVersionUID = -7302024110663488540L;

    private String address;

    public TopicSubscriptionFetchEvent() {
    }

    public TopicSubscriptionFetchEvent(String address) {
        this.address = address;
    }

    //setter && getter
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
