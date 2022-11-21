package org.kin.mqtt.broker.rule.impl;

import org.kin.mqtt.broker.rule.RuleChainContext;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.kin.mqtt.broker.rule.RuleNode;
import reactor.core.publisher.Mono;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class PredicateRule extends ScriptRule {
    public PredicateRule(RuleDefinition definition) {
        super(definition);
    }

    public PredicateRule(RuleDefinition definition, RuleNode next) {
        super(definition, next);
    }

    @Override
    protected Mono<Void> postExecScript(RuleChainContext context, Object result) {
        if (result instanceof Boolean) {
            if ((Boolean) result) {
                return next(context);
            }
        }

        //校验不通过, 直接 complete
        return Mono.empty();
    }
}
