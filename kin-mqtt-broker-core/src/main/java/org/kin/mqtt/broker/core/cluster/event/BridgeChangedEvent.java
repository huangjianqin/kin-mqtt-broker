package org.kin.mqtt.broker.core.cluster.event;

/**
 * 数据桥接更新事件
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
public class BridgeChangedEvent extends AbstractBridgeEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 1133753017864085240L;

    public static BridgeChangedEvent of(String bridgeName) {
        BridgeChangedEvent inst = new BridgeChangedEvent();
        inst.bridgeName = bridgeName;
        return inst;
    }
}
