package org.kin.mqtt.broker.rule;

import reactor.core.publisher.Mono;

/**
 * 规则链
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleChain implements RuleNode {
    /** 规则链名字 */
    private final String name;
    /** 规则链描述 */
    private final String desc;
    /** {@link  RuleNode}链表 */
    private final RuleNode root;

    public RuleChain(String name, String desc, RuleNode root) {
        this.name = name;
        this.desc = desc;
        this.root = root;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Mono<Void> execute(RuleChainContext context) {
        return root.execute(context);
    }

    //getter

    public String getDesc() {
        return desc;
    }

    public RuleNode getRoot() {
        return root;
    }
}
