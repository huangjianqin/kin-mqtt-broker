package org.kin.mqtt.broker.rule;

import org.kin.mqtt.broker.rule.action.ActionDefinition;

import java.util.Set;

/**
 * 规则定义
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public class RuleDefinition {
    /**
     * 规则名
     */
    private String name;
    /** 规则描述 */
    private String desc;
    /** sql */
    private String sql;
    /** 绑定的动作 */
    private Set<ActionDefinition> actionDefs;

    private RuleDefinition() {
    }

    /**
     * 添加动作
     *
     * @param actionDefinition 规则定义
     */
    public void addAction(ActionDefinition actionDefinition) {
        if (containsAction(actionDefinition)) {
            throw new IllegalStateException("action has registered, " + actionDefinition);
        }

        actionDefs.add(actionDefinition);
    }

    /**
     * 是否已经包含指定动作
     *
     * @param actionDefinition 动作定义
     * @return 是否已经包含指定动作
     */
    public boolean containsAction(ActionDefinition actionDefinition) {
        return actionDefs.contains(actionDefinition);
    }

    /**
     * 移除动作
     *
     * @param actionDefinition 动作定义
     */
    public boolean removeAction(ActionDefinition actionDefinition) {
        return actionDefs.remove(actionDefinition);
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

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Set<ActionDefinition> getActionDefs() {
        return actionDefs;
    }

    public void setActionDefs(Set<ActionDefinition> actionDefs) {
        this.actionDefs = actionDefs;
    }

    @Override
    public String toString() {
        return "RuleDefinition{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", sql='" + sql + '\'' +
                ", actionDefs=" + actionDefs +
                '}';
    }

    public static Builder builder() {
        return new Builder();
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

        public Builder sql(String sql) {
            ruleDefinition.sql = sql;
            return this;
        }

        public Builder actionDefs(Set<ActionDefinition> actionDefs) {
            ruleDefinition.actionDefs = actionDefs;
            return this;
        }

        public RuleDefinition build() {
            return ruleDefinition;
        }
    }
}
