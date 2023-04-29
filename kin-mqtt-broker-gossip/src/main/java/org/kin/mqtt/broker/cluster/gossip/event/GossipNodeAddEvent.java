package org.kin.mqtt.broker.cluster.gossip.event;

import io.scalecube.net.Address;
import org.kin.mqtt.broker.event.InternalMqttEvent;

/**
 * 新增gossip节点事件
 *
 * @author huangjianqin
 * @date 2023/4/25
 */
public class GossipNodeAddEvent implements InternalMqttEvent {
    /** gossip node address */
    private Address memberAddress;

    public GossipNodeAddEvent() {
    }

    public GossipNodeAddEvent(Address memberAddress) {
        this.memberAddress = memberAddress;
    }

    //setter && getter
    public Address getMemberAddress() {
        return memberAddress;
    }

    public void setMemberAddress(Address memberAddress) {
        this.memberAddress = memberAddress;
    }
}
