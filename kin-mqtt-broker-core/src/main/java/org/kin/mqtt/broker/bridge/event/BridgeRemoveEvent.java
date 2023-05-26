package org.kin.mqtt.broker.bridge.event;

import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;

import java.util.Arrays;
import java.util.List;

/**
 * 移除数据桥接事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public class BridgeRemoveEvent extends AbstractBridgeEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 1048367402310871011L;

    public static BridgeRemoveEvent of(String... bridgeNames) {
        BridgeRemoveEvent inst = new BridgeRemoveEvent();
        inst.bridgeNames = Arrays.asList(bridgeNames);
        return inst;
    }

    public static BridgeRemoveEvent of(List<String> bridgeNames) {
        BridgeRemoveEvent inst = new BridgeRemoveEvent();
        inst.bridgeNames = bridgeNames;
        return inst;
    }
}