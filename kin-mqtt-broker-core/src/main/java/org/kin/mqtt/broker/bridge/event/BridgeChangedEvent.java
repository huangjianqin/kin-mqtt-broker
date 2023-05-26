package org.kin.mqtt.broker.bridge.event;

import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 数据桥接更新事件
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
public class BridgeChangedEvent extends AbstractBridgeEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 1133753017864085240L;

    public static BridgeChangedEvent of(String... bridgeNames) {
        return of(Arrays.asList(bridgeNames));
    }

    public static BridgeChangedEvent of(List<String> bridgeNames) {
        BridgeChangedEvent inst = new BridgeChangedEvent();
        inst.bridgeNames = CollectionUtils.isNonEmpty(bridgeNames) ? bridgeNames : Collections.emptyList();
        return inst;
    }
}
