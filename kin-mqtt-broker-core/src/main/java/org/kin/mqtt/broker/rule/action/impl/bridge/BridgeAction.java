package org.kin.mqtt.broker.rule.action.impl.bridge;

import org.kin.mqtt.broker.bridge.Bridge;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.AbstractAction;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 数据桥接动作规则抽象实现
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public abstract class BridgeAction extends AbstractAction {
    protected BridgeAction(ActionConfiguration config) {
        super(config);
    }

    @Override
    public Mono<Void> execute(RuleContext context) {
        MqttBrokerContext brokerContext = context.getBrokerContext();
        String bridgeName = config.get(BridgeActionConstants.BRIDGE_KEY);
        Bridge bridge = brokerContext.getBridgeManager().getBridge(bridgeName);
        if (Objects.isNull(bridge)) {
            //找不到指定桥接实现, 则直接complete, 中断
            return Mono.error(new IllegalStateException(String.format("can not find bridge '%s'", bridgeName)));
        }
        preTransmit(context);
        return bridge.transmit(context.getAttrs());
    }

    /**
     * 桥接前的动作, 用于指定桥接参数
     *
     * @param context context 规则链上下文
     */
    protected void preTransmit(RuleContext context) {
        //default do nothing
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                super.toString() +
                '}';
    }
}
