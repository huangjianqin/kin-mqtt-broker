package org.kin.mqtt.broker.rule;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.rule.action.ActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.HttpBridgeActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.KafkaBridgeActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.MqttTopicActionDefinition;
import org.kin.mqtt.broker.rule.action.bridge.definition.RabbitMQBridgeActionDefinition;

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
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({@JsonSubTypes.Type(name = "http", value = HttpBridgeActionDefinition.class),
            @JsonSubTypes.Type(name = "kafka", value = KafkaBridgeActionDefinition.class),
            @JsonSubTypes.Type(name = "mqttTopic", value = MqttTopicActionDefinition.class),
            @JsonSubTypes.Type(name = "rabbitMQ", value = RabbitMQBridgeActionDefinition.class)})
    private Set<ActionDefinition> actionDefs = new CopyOnWriteArraySet<>();

    private RuleDefinition() {
    }

    /**
     * 检查配置是符合要求
     */
    public void check() {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "rule name must be not blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(sql), "rule sql must be not blank");
        for (ActionDefinition actionDef : actionDefs) {
            actionDef.check();
        }
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RuleDefinition that = (RuleDefinition) o;
        return Objects.equals(name, that.name) && Objects.equals(desc, that.desc) && Objects.equals(sql, that.sql) && Objects.equals(actionDefs, that.actionDefs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, desc, sql, actionDefs);
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

    //-----------------------------------------------------------------------------------------------------------------
    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final RuleDefinition ruleDefinition = new RuleDefinition();
        private final Set<ActionDefinition> actionDefs = new HashSet<>();

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

        public Builder actionDefs(Collection<ActionDefinition> actionDefs) {
            this.actionDefs.addAll(actionDefs);
            return this;
        }

        public Builder actionDefs(ActionDefinition... actionDefs) {
            return actionDefs(Arrays.asList(actionDefs));
        }

        public RuleDefinition build() {
            ruleDefinition.actionDefs = new CopyOnWriteArraySet<>(actionDefs);
            return ruleDefinition;
        }
    }
}
