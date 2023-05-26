package org.kin.mqtt.broker.core.cluster.event;

import org.kin.mqtt.broker.core.event.MqttEvent;

/**
 * 桥接相关事件
 * @author huangjianqin
 * @date 2023/5/26
 */
public abstract class AbstractBridgeEvent implements MqttEvent {
    /** 数据桥接名称 */
    protected String bridgeName;

    //setter && getter
    public String getBridgeName() {
        return bridgeName;
    }

    public void setBridgeName(String bridgeName) {
        this.bridgeName = bridgeName;
    }
}
