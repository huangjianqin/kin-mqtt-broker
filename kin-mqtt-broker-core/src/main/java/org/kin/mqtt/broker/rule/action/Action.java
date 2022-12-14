package org.kin.mqtt.broker.rule.action;

import org.kin.mqtt.broker.rule.RuleContext;
import reactor.core.publisher.Mono;

/**
 * 动作接口
 *
 * @author huangjianqin
 * @date 2022/12/16
 */
public interface Action {
    /**
     * 动作开始执行
     *
     * @param context 规则上下文
     * @return complete signal
     */
    Mono<Void> start(RuleContext context);

    /**
     * 返回动作定义
     *
     * @return 动作定义
     */
    ActionDefinition definition();
}
