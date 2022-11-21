package org.kin.mqtt.broker.rule.impl;

import org.kin.mqtt.broker.rule.ConditionLessRuleNode;
import org.kin.mqtt.broker.rule.RuleChainContext;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.kin.mqtt.broker.rule.RuleNode;
import reactor.core.publisher.Mono;

/**
 * todo
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class BridgeRule extends ConditionLessRuleNode {
    public BridgeRule(RuleDefinition definition) {
        super(definition);
    }

    public BridgeRule(RuleDefinition definition, RuleNode next) {
        super(definition, next);
    }

    @Override
    protected Mono<Void> execute0(RuleChainContext context) {
        return null;
    }
}
