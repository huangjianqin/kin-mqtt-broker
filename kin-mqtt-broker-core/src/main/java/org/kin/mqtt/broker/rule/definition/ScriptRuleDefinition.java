package org.kin.mqtt.broker.rule.definition;

/**
 * jexl3脚本规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see ScriptRuleDefinition
 * @see org.kin.mqtt.broker.rule.impl.PredicateRule
 */
public final class ScriptRuleDefinition extends RuleDefinition {
    /** 规则表达式脚本 */
    private String script;

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder extends RuleDefinition.Builder<ScriptRuleDefinition> {
        protected Builder() {
            super(new ScriptRuleDefinition());
        }

        public Builder script(String script) {
            definition.script = script;
            return this;
        }
    }

    //setter && getter
    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    @Override
    public String toString() {
        return "ScriptRuleDefinition{" +
                super.toString() +
                "script='" + script + '\'' +
                "} ";
    }
}
