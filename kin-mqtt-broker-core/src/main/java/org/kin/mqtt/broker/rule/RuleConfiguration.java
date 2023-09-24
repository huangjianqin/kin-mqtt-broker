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
    private Set<ActionConfiguration> actionConfigs = new CopyOnWriteArraySet<>();

    private RuleConfiguration() {
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
     * @param actionConfiguration 规则配置
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
     * @param actionConfig 动作配置
     * @return 是否已经包含指定动作
     */
    public boolean containsAction(ActionConfiguration actionConfig) {
        return actionConfigs.contains(actionConfig);
    }

    /**
     * 移除动作
     *
     * @param actionConfig 动作配置
     */
    public boolean removeAction(ActionConfiguration actionConfig) {
        return actionConfigs.remove(actionConfig);
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
        RuleConfiguration that = (RuleConfiguration) o;
        return Objects.equals(name, that.name) && Objects.equals(desc, that.desc) && Objects.equals(sql, that.sql) && Objects.equals(actionConfigs, that.actionConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, desc, sql, actionConfigs);
    }

    @Override
    public String toString() {
        return "RuleConfiguration{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", sql='" + sql + '\'' +
                ", actionConfigs=" + actionConfigs +
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

        public Builder actionConfigs(Collection<ActionConfiguration> actionConfigs) {
            this.actionConfigs.addAll(actionConfigs);
            return this;
        }

        public Builder actionConfigs(ActionConfiguration... actionConfigs) {
            return actionConfigs(Arrays.asList(actionConfigs));
        }

        public RuleConfiguration build() {
            ruleConfiguration.actionConfigs = new CopyOnWriteArraySet<>(actionConfigs);
            return ruleConfiguration;
        }
    }
}
