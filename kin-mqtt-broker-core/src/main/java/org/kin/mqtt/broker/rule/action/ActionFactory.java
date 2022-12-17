package org.kin.mqtt.broker.rule.action;

import java.util.Collection;

/**
 * {@link Action}实现构造工厂
 * 用抽象类的原因是如果用接口实现, 则可以简化成lambda, 这样子{@link Actions#registerActions(Collection)} )}获取不了泛型信息
 *
 * @author huangjianqin
 * @date 2022/12/17
 */
public abstract class ActionFactory<AD extends ActionDefinition, A extends Action> {
    /**
     * {@link Action}实现构造逻辑
     *
     * @param ad 动作定义
     * @return {@link Action}实例
     */
    public abstract A create(AD ad);
}
