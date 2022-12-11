package org.kin.mqtt.broker.rule.definition;

import org.kin.mqtt.broker.rule.RuleType;

/**
 * 规则定义
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public abstract class RuleDefinition {
    /**
     * 规则名
     */
    private String name;
    /** 规则描述 */
    private String desc;
    /** 规则类型 */
    private RuleType type;

    /** builder **/
    @SuppressWarnings("unchecked")
    public static abstract class Builder<RD extends RuleDefinition> {
        protected final RD definition;

        protected Builder(RD definition) {
            this.definition = definition;
        }

        public <B extends Builder<RD>> B name(String name) {
            definition.setName(name);
            return (B) this;
        }

        public <B extends Builder<RD>> B desc(String desc) {
            definition.setDesc(desc);
            return (B) this;
        }

        public <B extends Builder<RD>> B type(RuleType type) {
            definition.setType(type);
            return (B) this;
        }

        public RD build() {
            return definition;
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

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", type=" + type +
                ", ";
    }
}
