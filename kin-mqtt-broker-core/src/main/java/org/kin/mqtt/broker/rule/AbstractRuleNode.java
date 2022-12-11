package org.kin.mqtt.broker.rule;

import org.kin.mqtt.broker.rule.definition.RuleDefinition;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public abstract class AbstractRuleNode<RD extends RuleDefinition> implements RuleNode {
    protected final RD definition;
    /** 下一规则节点执行 */
    private final RuleNode next;

    protected AbstractRuleNode(RD definition) {
        this(definition, null);
    }

    protected AbstractRuleNode(RD definition, RuleNode next) {
        this.definition = definition;
        this.next = next;
    }

    /**
     * 执行下一规则(如果存在的话)
     *
     * @param context 规则链上下文
     * @return 执行complete signal
     */
    protected Mono<Void> next(RuleChainContext context) {
        if (Objects.nonNull(next)) {
            //执行下一规则
            return next.execute(context);
        }

        //complete
        return Mono.empty();
    }

    @Override
    public final String name() {
        return definition.getName();
    }
}
