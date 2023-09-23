package org.kin.mqtt.broker.bridge;

import org.kin.framework.utils.SPI;

/**
 * {@link Bridge}实现构造工厂
 *
 * @author huangjianqin
 * @date 2023/5/26
 */
@SPI("bridgeFactory")
public interface BridgeFactory<B extends Bridge> {
    /**
     * {@link Bridge}实现构造逻辑
     *
     * @param config 动作配置
     * @return {@link Bridge}实例
     */
    B create(BridgeConfiguration config);
}
