package org.kin.mqtt.broker.rule.impl;

import org.kin.mqtt.broker.rule.AbstractRuleNode;
import org.kin.mqtt.broker.rule.Jexl3Utils;
import org.kin.mqtt.broker.rule.RuleChainContext;
import org.kin.mqtt.broker.rule.RuleNode;
import org.kin.mqtt.broker.rule.definition.ScriptRuleDefinition;
import reactor.core.publisher.Mono;

/**
 * 执行脚本的规则
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public class ScriptRule extends AbstractRuleNode<ScriptRuleDefinition> {
    public ScriptRule(ScriptRuleDefinition definition) {
        super(definition);
    }

    public ScriptRule(ScriptRuleDefinition definition, RuleNode next) {
        super(definition, next);
    }

    @Override
    public final Mono<Void> execute(RuleChainContext context) {
        Object result = Jexl3Utils.execScript(definition.getScript(), c -> {
            //脚本逻辑可以访问RuleChainContext
            c.set("chainContext", context);
            context.getAttrs().forEach(c::set);
        });
        return postExecScript(context, result);
    }

    /**
     * 定义脚本执行完后的结果处理
     *
     * @param context 规则链上下文
     * @param result  脚本执行结果
     * @return node complete signal
     */
    protected Mono<Void> postExecScript(RuleChainContext context, Object result) {
        //默认执行直接下一规则
        return next(context);
    }
}
