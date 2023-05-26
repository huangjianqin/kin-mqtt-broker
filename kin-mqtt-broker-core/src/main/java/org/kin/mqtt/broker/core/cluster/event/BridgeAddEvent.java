package org.kin.mqtt.broker.core.cluster.event;

/**
 * 新增数据桥接事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public class BridgeAddEvent extends AbstractBridgeEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 1133753017864085240L;

    public static BridgeAddEvent of(String bridgeName) {
        BridgeAddEvent inst = new BridgeAddEvent();
        inst.bridgeName = bridgeName;
        return inst;
    }
}
