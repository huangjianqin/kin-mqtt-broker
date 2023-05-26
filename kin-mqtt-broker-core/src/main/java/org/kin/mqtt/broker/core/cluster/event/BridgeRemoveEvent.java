package org.kin.mqtt.broker.core.cluster.event;

/**
 * 移除数据桥接事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public class BridgeRemoveEvent extends AbstractBridgeEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 1048367402310871011L;

    public static BridgeRemoveEvent of(String bridgeName) {
        BridgeRemoveEvent inst = new BridgeRemoveEvent();
        inst.bridgeName = bridgeName;
        return inst;
    }
}