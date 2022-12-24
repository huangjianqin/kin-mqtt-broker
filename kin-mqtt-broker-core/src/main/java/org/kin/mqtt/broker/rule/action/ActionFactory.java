package org.kin.mqtt.broker.rule.action;

/**
 * {@link Action}实现构造工厂
 *
 * @author huangjianqin
 * @date 2022/12/17
 */
@FunctionalInterface
public interface ActionFactory<AD extends ActionDefinition, A extends Action> {
    /**
     * {@link Action}实现构造逻辑
     *
     * @param ad 动作定义
     * @return {@link Action}实例
     */
    A create(AD ad);
}
