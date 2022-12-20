package org.kin.mqtt.broker.bridge;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.Closeable;
import org.kin.mqtt.broker.cluster.event.BridgeAddEvent;
import org.kin.mqtt.broker.cluster.event.BridgeRemoveEvent;
import org.kin.mqtt.broker.core.MqttBrokerContext;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/12/11
 */
public final class BridgeManager implements Closeable {
    /** mqtt broker context */
    private MqttBrokerContext brokerContext;
    /** key -> bridge name, value -> {@link Bridge} */
    private final Map<String, Bridge> bridgeMap = new NonBlockingHashMap<>();

    /**
     * 初始化mqtt broker context
     *
     * @param brokerContext mqtt broker context
     */
    public void initBrokerContext(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
    }

    /**
     * 添加数据桥接实现
     *
     * @param bridge 数据桥接实现
     */
    public void addBridge(Bridge bridge) {
        String bridgeName = bridge.name();
        if (bridgeMap.containsKey(bridgeName)) {
            throw new IllegalStateException(String.format("bridge name '%s' conflict!!", bridgeName));
        }

        bridgeMap.put(bridgeName, bridge);
        brokerContext.broadcastClusterEvent(BridgeAddEvent.of(bridgeName));
    }

    /**
     * 移除数据桥接实现
     *
     * @param bridgeName 数据桥接名
     * @return 是否移除成功
     */
    public boolean removeBridge(String bridgeName) {
        Bridge removed = bridgeMap.remove(bridgeName);
        if (Objects.nonNull(removed)) {
            brokerContext.broadcastClusterEvent(BridgeRemoveEvent.of(bridgeName));
            removed.close();
        }
        return Objects.nonNull(removed);
    }

    /**
     * 根据数据桥接名字获取{@link  Bridge}实例
     *
     * @param bridgeName 桥接名字
     * @return {@link  Bridge}实例
     */
    @Nullable
    public Bridge getBridge(String bridgeName) {
        return bridgeMap.get(bridgeName);
    }

    @Override
    public void close() {
        for (Bridge bridge : bridgeMap.values()) {
            bridge.close();
        }
    }
}
