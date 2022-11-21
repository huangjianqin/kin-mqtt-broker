package org.kin.mqtt.broker.rule;

import java.util.List;

/**
 * 规则链定义
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleChainDefinition {
    /** 规则命名 */
    private String ruleName;
    /** 规则链定义 */
    private List<RuleDefinition> chain;

    //setter && getter
    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public List<RuleDefinition> getChain() {
        return chain;
    }

    public void setChain(List<RuleDefinition> chain) {
        this.chain = chain;
    }
}
