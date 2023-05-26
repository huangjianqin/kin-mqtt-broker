package org.kin.mqtt.broker.rule.action;

import org.kin.mqtt.broker.rule.RuleContext;
import reactor.core.publisher.Mono;

/**
 * 动作接口
 *
 * 使用者自定义{@link Action}实现时, 要使用{@link Actions#registerActionFactory(Class, ActionFactory)}注册加载逻辑
 *
 * @author huangjianqin
 * @date 2022/12/16
 */
public interface Action {
    /**
     * 执行动作
     *
     * @param context 规则上下文
     * @return complete signal
     */
    Mono<Void> execute(RuleContext context);

    /**
     * 返回动作定义
     *
     * @return 动作定义
     */
    ActionDefinition definition();
}
