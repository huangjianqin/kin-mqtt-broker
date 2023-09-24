package org.kin.mqtt.broker.rule;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.ActionConfiguration;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 规则配置
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public class RuleConfiguration {
    /** 规则名 */
    private String name;
    /** 规则描述 */
    private String desc;
    /** sql */
    private String sql;
    /** 绑定的动作 */
    private Set<ActionConfiguration> actions = new CopyOnWriteArraySet<>();

    private RuleConfiguration() {
    }

    /**
     * 检查配置是符合要求
     */
    public void check() {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "rule name must be not blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(sql), "rule sql must be not blank");
        actions.forEach(ActionConfiguration::check);
    }

    /**
     * 添加动作
     *
     * @param actionConfiguration 规则配置
     */
    public void addAction(ActionConfiguration actionConfiguration) {
        if (containsAction(actionConfiguration)) {
            throw new IllegalStateException("action has registered, " + actionConfiguration);
        }

        actions.add(actionConfiguration);
    }

    /**
     * 是否已经包含指定动作
     *
     * @param actionConfig 动作配置
     * @return 是否已经包含指定动作
     */
    public boolean containsAction(ActionConfiguration actionConfig) {
        return actions.contains(actionConfig);
    }

    /**
     * 移除动作
     *
     * @param actionConfig 动作配置
     */
    public boolean removeAction(ActionConfiguration actionConfig) {
        return actions.remove(actionConfig);
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

    public Set<ActionConfiguration> getActions() {
        return actions;
    }

    public void setActions(Set<ActionConfiguration> actions) {
        this.actions = actions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RuleConfiguration that = (RuleConfiguration) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(desc, that.desc) &&
                Objects.equals(sql, that.sql) &&
                Objects.equals(actions, that.actions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, desc, sql, actions);
    }

    @Override
    public String toString() {
        return "RuleConfiguration{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", sql='" + sql + '\'' +
                ", actions=" + actions +
                '}';
    }

    //-----------------------------------------------------------------------------------------------------------------
    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final RuleConfiguration ruleConfiguration = new RuleConfiguration();
        private final Set<ActionConfiguration> actionConfigs = new HashSet<>();

        public Builder name(String name) {
            ruleConfiguration.name = name;
            return this;
        }

        public Builder desc(String desc) {
            ruleConfiguration.desc = desc;
            return this;
        }

        public Builder sql(String sql) {
            ruleConfiguration.sql = sql;
            return this;
        }

        public Builder actions(Collection<ActionConfiguration> actionConfigs) {
            this.actionConfigs.addAll(actionConfigs);
            return this;
        }

        public Builder actions(ActionConfiguration... actionConfigs) {
            return actions(Arrays.asList(actionConfigs));
        }

        public Builder action(ActionConfiguration actionConfig) {
            return actions(actionConfig);
        }

        public RuleConfiguration build() {
            ruleConfiguration.actions = new CopyOnWriteArraySet<>(actionConfigs);
            return ruleConfiguration;
        }
    }
}
