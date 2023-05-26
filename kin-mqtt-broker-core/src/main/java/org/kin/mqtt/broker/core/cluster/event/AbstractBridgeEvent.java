package org.kin.mqtt.broker.core.cluster.event;

import org.kin.mqtt.broker.core.event.MqttEvent;

import java.util.List;

/**
 * 桥接相关事件
 * @author huangjianqin
 * @date 2023/5/26
 */
public abstract class AbstractBridgeEvent implements MqttEvent {
    /** 数据桥接名称 */
    protected List<String> bridgeNames;

    //setter && getter
    public List<String> getBridgeNames() {
        return bridgeNames;
    }

    public void setBridgeNames(List<String> bridgeNames) {
        this.bridgeNames = bridgeNames;
    }
}
