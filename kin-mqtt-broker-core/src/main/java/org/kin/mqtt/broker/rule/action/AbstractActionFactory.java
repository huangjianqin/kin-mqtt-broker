package org.kin.mqtt.broker.rule.action;

/**
 * @author huangjianqin
 * @date 2023/9/24
 */
public abstract class AbstractActionFactory<A extends Action> implements ActionFactory<A> {
    /**
     * 动作配置检查
     *
     * @param config 动作配置
     */
    protected void checkConfig(ActionConfiguration config) {
        //default do nothing
    }

    @Override
    public final A create(ActionConfiguration config) {
        checkConfig(config);
        return create0(config);
    }

    protected abstract A create0(ActionConfiguration config);
}
