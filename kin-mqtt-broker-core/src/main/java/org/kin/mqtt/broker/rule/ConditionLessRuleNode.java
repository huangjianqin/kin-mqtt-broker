package org.kin.mqtt.broker.rule;

import reactor.core.publisher.Mono;

/**
 * 无条件控制节点, 即该节点执行完, 继续执行下一节点
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public abstract class ConditionLessRuleNode extends AbstractRuleNode {
    protected ConditionLessRuleNode(RuleDefinition definition) {
        super(definition);
    }

    protected ConditionLessRuleNode(RuleDefinition definition, RuleNode next) {
        super(definition, next);
    }

    @Override
    public Mono<Void> execute(RuleChainContext context) {
        return execute0(context).flatMap(v -> next(context));
    }

    /**
     * 规则执行
     *
     * @param context 规则链上下文
     * @return 执行complete signal
     */
    protected abstract Mono<Void> execute0(RuleChainContext context);
}