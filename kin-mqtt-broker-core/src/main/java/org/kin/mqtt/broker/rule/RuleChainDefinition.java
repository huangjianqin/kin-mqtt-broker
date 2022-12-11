package org.kin.mqtt.broker.rule;

import org.kin.mqtt.broker.rule.definition.RuleDefinition;

import java.util.LinkedList;
import java.util.List;

/**
 * 规则链定义
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleChainDefinition {
    /** 规则命名 */
    private String name;
    /** 规则链描述 */
    private String desc;
    /** 规则链定义 */
    private List<RuleDefinition> chain = new LinkedList<>();

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(String name, List<RuleDefinition> definitions) {
        return new Builder().name(name).rules(definitions);
    }

    public static Builder builder(String name, String desc, List<RuleDefinition> definitions) {
        return new Builder().name(name).desc(desc).rules(definitions);
    }

    /** builder **/
    public static class Builder {
        private final RuleChainDefinition ruleChainDefinition = new RuleChainDefinition();

        public Builder name(String name) {
            ruleChainDefinition.name = name;
            return this;
        }

        public Builder desc(String desc) {
            ruleChainDefinition.desc = desc;
            return this;
        }

        public Builder rules(List<RuleDefinition> definitions) {
            ruleChainDefinition.chain.addAll(definitions);
            return this;
        }

        public Builder rule(RuleDefinition definition) {
            ruleChainDefinition.chain.add(definition);
            return this;
        }

        public RuleChainDefinition build() {
            return ruleChainDefinition;
        }
    }

    //setter && getter
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public List<RuleDefinition> getChain() {
        return chain;
    }

    public void setChain(List<RuleDefinition> chain) {
        this.chain = chain;
    }

    @Override
    public String toString() {
        return "RuleChainDefinition{" +
                "name='" + name + '\'' +
                ", chain=" + chain +
                '}';
    }
}
