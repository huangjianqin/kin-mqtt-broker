package org.kin.mqtt.broker.rule;

/**
 * 规则定义
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleDefinition {
    /**
     * 规则名
     */
    private String name;
    /** 规则描述 */
    private String desc;
    /** 规则类型 */
    private RuleType type;
    /** 规则表达式脚本 */
    private String script;

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(String name, RuleType type) {
        return new Builder().name(name).type(type);
    }

    public static Builder builder(String name, String desc, RuleType type) {
        return new Builder().name(name).desc(desc).type(type);
    }

    /** builder **/
    public static class Builder {
        private final RuleDefinition ruleDefinition = new RuleDefinition();

        public Builder name(String name) {
            ruleDefinition.name = name;
            return this;
        }

        public Builder desc(String desc) {
            ruleDefinition.desc = desc;
            return this;
        }

        public Builder type(RuleType type) {
            ruleDefinition.type = type;
            return this;
        }

        public Builder script(String script) {
            ruleDefinition.script = script;
            return this;
        }

        public RuleDefinition build() {
            return ruleDefinition;
        }
    }

    //setter && getter
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RuleType getType() {
        return type;
    }

    public void setType(RuleType type) {
        this.type = type;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    @Override
    public String toString() {
        return "RuleDefinition{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", type=" + type +
                ", script='" + script + '\'' +
                '}';
    }
}
