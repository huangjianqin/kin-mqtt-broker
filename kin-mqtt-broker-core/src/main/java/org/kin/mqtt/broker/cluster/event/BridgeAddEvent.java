package org.kin.mqtt.broker.cluster.event;

/**
 * 新增数据桥接事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public class BridgeAddEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 1133753017864085240L;
    /** 数据桥接名称 */
    private String bridgeName;

    public static BridgeAddEvent of(String bridgeName) {
        BridgeAddEvent inst = new BridgeAddEvent();
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
