package org.kin.mqtt.broker.core.cluster.event;

/**
 * 移除数据桥接事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public class BridgeRemoveEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 1048367402310871011L;
    /** 数据桥接名称 */
    private String bridgeName;

    public static BridgeRemoveEvent of(String bridgeName) {
        BridgeRemoveEvent inst = new BridgeRemoveEvent();
        inst.bridgeName = bridgeName;
        return inst;
    }

    //setter && getter
    public String getBridgeName() {
        return bridgeName;
    }

    public void setBridgeName(String bridgeName) {
        this.bridgeName = bridgeName;
    }
}