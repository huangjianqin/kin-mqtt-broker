package org.kin.mqtt.broker.rule.action.bridge;

import org.kin.mqtt.broker.bridge.Bridge;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.rule.RuleContext;
import org.kin.mqtt.broker.rule.action.Action;
import org.kin.mqtt.broker.rule.action.ActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.BridgeActionDefinition;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 数据桥接动作规则抽象实现
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public abstract class BridgeAction<BAD extends BridgeActionDefinition> implements Action {
    /** 桥接动作定义 */
    protected final BAD definition;

    protected BridgeAction(BAD definition) {
        this.definition = definition;
    }

    @Override
    public Mono<Void> execute(RuleContext context) {
        MqttBrokerContext brokerContext = context.getBrokerContext();
        String bridgeName = definition.getBridgeName();
        Bridge bridge = brokerContext.getBridgeManager().getBridge(bridgeName);
        if (Objects.isNull(bridge)) {
            //找不到指定桥接实现, 则直接complete, 中断
            return Mono.error(new IllegalStateException(String.format("can not find bridge '%s'", bridgeName)));
        }
        preTransmit(context);
        return bridge.transmit(context.getAttrs());
    }

    @Override
    public ActionDefinition definition() {
        return definition;
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
                "definition=" + definition +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BridgeAction)) {
            return false;
        }
        BridgeAction<?> that = (BridgeAction<?>) o;
        return Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(definition);
    }
}
