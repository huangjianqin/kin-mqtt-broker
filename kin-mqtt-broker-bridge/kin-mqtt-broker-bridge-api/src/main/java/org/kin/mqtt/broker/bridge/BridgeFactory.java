package org.kin.mqtt.broker.bridge;

import org.kin.framework.utils.SPI;
import org.kin.mqtt.broker.bridge.definition.BridgeDefinition;

/**
 * {@link Bridge}实现构造工厂
 * @author huangjianqin
 * @date 2023/5/26
 */
@SPI("bridgeFactory")
public interface BridgeFactory <BD extends BridgeDefinition, B extends Bridge> {
    /**
     * {@link Bridge}实现构造逻辑
     *
     * @param bd 动作定义
     * @return {@link Bridge}实例
     */
    B create(BD bd);
}
