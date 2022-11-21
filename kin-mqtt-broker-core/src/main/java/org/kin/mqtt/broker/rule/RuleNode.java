package org.kin.mqtt.broker.rule;

import reactor.core.publisher.Mono;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public interface RuleNode {
    /** 该节点规则名 */
    String name();

    /**
     * 规则执行
     *
     * @param context 规则链上下文
     * @return 执行complete signal
     */
    Mono<Void> execute(RuleChainContext context);
}
