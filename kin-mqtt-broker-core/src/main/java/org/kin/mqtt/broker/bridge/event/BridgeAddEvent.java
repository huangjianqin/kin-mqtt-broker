package org.kin.mqtt.broker.bridge.event;

import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.cluster.event.MqttClusterEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 新增数据桥接事件
 *
 * @author huangjianqin
 * @date 2022/12/21
 */
public class BridgeAddEvent extends AbstractBridgeEvent implements MqttClusterEvent {
    private static final long serialVersionUID = 1133753017864085240L;

    public static BridgeAddEvent of(String... bridgeNames) {
        return of(Arrays.asList(bridgeNames));
    }

    public static BridgeAddEvent of(List<String> bridgeNames) {
        BridgeAddEvent inst = new BridgeAddEvent();
        inst.bridgeNames = CollectionUtils.isNonEmpty(bridgeNames) ? bridgeNames : Collections.emptyList();
        return inst;
    }
}
