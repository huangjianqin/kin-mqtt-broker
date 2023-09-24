package org.kin.mqtt.broker.rule;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 规则定义
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public class RuleDefinition {
    /** 规则名 */
    private String name;
    /** 规则描述 */
    private String desc;
    /** sql */
    private String sql;
    /** 绑定的动作 */
    private Set<ActionConfiguration> actionConfigs = new CopyOnWriteArraySet<>();

    private RuleDefinition() {
    }

    /**
     * 检查配置是符合要求
     */
    public void check() {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "rule name must be not blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(sql), "rule sql must be not blank");
        actionConfigs.forEach(ActionConfiguration::check);
    }

    /**
     * 添加动作
     *
     * @param actionConfiguration 规则定义
     */
    public void addAction(ActionConfiguration actionConfiguration) {
        if (containsAction(actionConfiguration)) {
            throw new IllegalStateException("action has registered, " + actionConfiguration);
        }

        actionConfigs.add(actionConfiguration);
    }

    /**
     * 是否已经包含指定动作
     *
     * @param actionConfiguration 动作定义
     * @return 是否已经包含指定动作
     */
    public boolean containsAction(ActionConfiguration actionConfiguration) {
        return actionConfigs.contains(actionConfiguration);
    }

    /**
     * 移除动作
     *
     * @param actionConfiguration 动作定义
     */
    public boolean removeAction(ActionConfiguration actionConfiguration) {
        return actionConfigs.remove(actionConfiguration);
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

    public Set<ActionConfiguration> getActionConfigs() {
        return actionConfigs;
    }

    public void setActionConfigs(Set<ActionConfiguration> actionConfigs) {
        this.actionConfigs = actionConfigs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RuleDefinition that = (RuleDefinition) o;
        return Objects.equals(name, that.name) && Objects.equals(desc, that.desc) && Objects.equals(sql, that.sql) && Objects.equals(actionConfigs, that.actionConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, desc, sql, actionConfigs);
    }

    @Override
    public String toString() {
        return "RuleDefinition{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", sql='" + sql + '\'' +
                ", actionDefs=" + actionConfigs +
                '}';
    }

    //-----------------------------------------------------------------------------------------------------------------
    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final RuleDefinition ruleDefinition = new RuleDefinition();
        private final Set<ActionConfiguration> actionConfigs = new HashSet<>();

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

        public Builder actionConfigs(Collection<ActionConfiguration> actionDefs) {
            this.actionConfigs.addAll(actionDefs);
            return this;
        }

        public Builder actionConfigs(ActionConfiguration... actionDefs) {
            return actionConfigs(Arrays.asList(actionDefs));
        }

        public RuleDefinition build() {
            ruleDefinition.actionConfigs = new CopyOnWriteArraySet<>(actionConfigs);
            return ruleDefinition;
        }
    }
}
