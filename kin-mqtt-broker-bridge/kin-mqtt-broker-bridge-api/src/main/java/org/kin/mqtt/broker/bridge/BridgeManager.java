package org.kin.mqtt.broker.bridge;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.Closeable;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/12/11
 */
public final class BridgeManager implements Closeable {
    /** key -> bridge name, value -> {@link Bridge} */
    private final Map<String, Bridge> bridgeMap = new NonBlockingHashMap<>();

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
    }

    /**
     * 移除数据桥接实现
     *
     * @param bridgeName 数据桥接名
     * @return 是否移除成功
     */
    public boolean removeBridge(String bridgeName) {
        return Objects.nonNull(bridgeMap.remove(bridgeName));
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
