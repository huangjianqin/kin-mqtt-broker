package org.kin.mqtt.broker.rule.impl;

import org.kin.mqtt.broker.bridge.Bridge;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.rule.RuleChainContext;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.kin.mqtt.broker.rule.RuleNode;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 数据桥接规则实现
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class BridgeRule extends ScriptRule {
    public BridgeRule(RuleDefinition definition) {
        super(definition);
    }

    public BridgeRule(RuleDefinition definition, RuleNode next) {
        super(definition, next);
    }

    /**
     * 根据返回值选择桥接来转发数据
     *
     * @param context 规则链上下文
     * @param result  必须是[{桥接类型}, {桥接名字}]
     * @return 数据桥接complete signal
     */
    @Override
    protected Mono<Void> postExecScript(RuleChainContext context, Object result) {
        if (Objects.nonNull(result) && result instanceof String[]) {
            String[] typeAndName = (String[]) result;
            MqttBrokerContext brokerContext = context.getBrokerContext();
            Bridge bridge = brokerContext.getBridge(BridgeType.getByName(typeAndName[0]), typeAndName[1]);
            if (Objects.nonNull(bridge)) {
                return bridge.transmit(context.getAttrs());
            }
            //找不到指定桥接实现, 则直接complete, 中断
            return Mono.error(new IllegalStateException(String.format("can not find bridge '%s' for type '%s'", typeAndName[1], typeAndName[0])));
        }
        //脚本结果不对, 则直接complete, 中断
        return Mono.error(new IllegalStateException("rule '%s' script result is not right, must be [{桥接类型}, {桥接名字}]"));
    }
}
