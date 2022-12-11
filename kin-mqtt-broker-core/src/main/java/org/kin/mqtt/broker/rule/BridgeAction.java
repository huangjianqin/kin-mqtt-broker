package org.kin.mqtt.broker.rule;

import org.kin.mqtt.broker.bridge.Bridge;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.rule.definition.BridgeActionDefinition;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 数据桥接动作规则抽象实现
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public abstract class BridgeAction<BAD extends BridgeActionDefinition> extends ConditionLessRuleNode<BAD> {
    protected BridgeAction(BAD definition) {
        super(definition);
    }

    protected BridgeAction(BAD definition, RuleNode next) {
        super(definition, next);
    }

    /**
     * 根据返回值选择桥接来转发数据
     *
     * @param context 规则链上下文
     * @return 数据桥接complete signal
     */
    @Override
    protected final Mono<Void> execute0(RuleChainContext context) {
        MqttBrokerContext brokerContext = context.getBrokerContext();
        BridgeType bridgeType = type();
        String bridgeName = definition.getBridgeName();
        Bridge bridge = brokerContext.getBridge(bridgeType, bridgeName);
        if (Objects.isNull(bridge)) {
            //找不到指定桥接实现, 则直接complete, 中断
            return Mono.error(new IllegalStateException(String.format("can not find bridge '%s' for type '%s'", bridgeType, bridgeName)));
        }
        return bridge.transmit(context.getAttrs());
    }

    /**
     * 桥接前的动作, 用于指定桥接参数
     *
     * @param context context 规则链上下文
     */
    protected void preTransmit(RuleChainContext context) {
        //default do nothing
    }

    /**
     * 指定桥接类型
     *
     * @return 桥接类型
     */
    protected abstract BridgeType type();
}
