package org.kin.mqtt.broker.rule;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.mqtt.broker.cluster.event.RuleActionAddEvent;
import org.kin.mqtt.broker.cluster.event.RuleActionRemoveEvent;
import org.kin.mqtt.broker.cluster.event.RuleAddEvent;
import org.kin.mqtt.broker.cluster.event.RuleRemoveEvent;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.rule.action.ActionDefinition;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleManager {
    /** broker context */
    private final MqttBrokerContext brokerContext;
    /** key -> rule name, value -> {@link Rule} */
    private final Map<String, Rule> rules = new NonBlockingHashMap<>();

    public RuleManager(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
    }

    /**
     * 批量添加规则
     *
     * @param definitions 规则定义list
     */
    public void addRules(List<RuleDefinition> definitions) {
        for (RuleDefinition definition : definitions) {
            addRule(definition);
        }
    }

    /**
     * 添加规则
     *
     * @param definition 规则定义
     */
    public void addRule(RuleDefinition definition) {
        String name = definition.getName();
        if (rules.containsKey(name)) {
            throw new IllegalStateException(String.format("rule '%s' has registered", name));
        }

        rules.put(name, new Rule(definition));
        brokerContext.broadcastClusterEvent(RuleAddEvent.of(definition.getName()));
    }

    /**
     * 移除规则, 如果没有则抛异常
     *
     * @param name 规则定义
     */
    public void removeRule(String name) {
        Rule removed = rules.remove(name);
        if (Objects.isNull(removed)) {
            throw new IllegalStateException(String.format("can not find rule '%s'", name));
        }

        removed.dispose();
        brokerContext.broadcastClusterEvent(RuleRemoveEvent.of(name));
    }

    /**
     * 获取规则, 如果没有则抛异常
     *
     * @param name 规则定义
     * @return {@link Rule}实例
     */
    private Rule getRuleOrThrow(String name) {
        Rule rule = rules.get(name);
        if (Objects.isNull(rule)) {
            throw new IllegalStateException(String.format("can not find rule '%s'", name));
        }

        return rule;
    }

    /**
     * 添加动作
     *
     * @param name             规则名字
     * @param actionDefinition 动作定义
     */
    public void addAction(String name, ActionDefinition actionDefinition) {
        Rule rule = getRuleOrThrow(name);
        rule.addAction(actionDefinition);
        brokerContext.broadcastClusterEvent(RuleActionAddEvent.of(name));
    }

    /**
     * 移除动作
     *
     * @param name             规则名字
     * @param actionDefinition 动作定义
     */
    public boolean removeAction(String name, ActionDefinition actionDefinition) {
        Rule rule = getRuleOrThrow(name);
        boolean result = rule.removeAction(actionDefinition);
        if (result) {
            brokerContext.broadcastClusterEvent(RuleActionRemoveEvent.of(name));
        }
        return result;
    }

    /**
     * 根据名字匹配规则定义
     *
     * @param name 规则名字
     * @return 规则定义
     */
    @Nullable
    public RuleDefinition getRuleDefinition(String name) {
        Rule rule = rules.get(name);
        if (Objects.nonNull(rule)) {
            return rule.getDefinition();
        }
        return null;
    }

    /**
     * 获取所有规则定义
     *
     * @return 所有规则定义
     */
    public Collection<RuleDefinition> getAllRuleDefinition() {
        return rules.values().stream().map(Rule::getDefinition).collect(Collectors.toList());
    }

    /**
     * 获取所有规则实现
     *
     * @return 所有规则实现
     */
    public Collection<Rule> getAllRule() {
        return new ArrayList<>(rules.values());
    }
}
