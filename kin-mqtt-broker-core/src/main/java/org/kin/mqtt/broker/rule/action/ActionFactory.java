package org.kin.mqtt.broker.rule.action;

import org.kin.framework.utils.SPI;

/**
 * {@link Action}实现构造工厂
 *
 * @author huangjianqin
 * @date 2022/12/17
 */
@SPI(alias = "actionFactory")
@FunctionalInterface
public interface ActionFactory<A extends Action> {
    /**
     * {@link Action}实现构造逻辑
     *
     * @param config 动作配置
     * @return {@link Action}实例
     */
    A create(ActionConfiguration config);
}
