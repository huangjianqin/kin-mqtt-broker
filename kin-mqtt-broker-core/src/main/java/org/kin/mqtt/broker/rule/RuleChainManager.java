package org.kin.mqtt.broker.rule;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.mqtt.broker.rule.impl.BridgeRule;
import org.kin.mqtt.broker.rule.impl.PredicateRule;
import org.kin.mqtt.broker.rule.impl.ScriptRule;
import org.kin.mqtt.broker.rule.impl.TopicRule;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class RuleChainManager {
    /** key -> rule name, value -> {@link RuleChain} */
    private final Map<String, RuleChain> ruleChains = new NonBlockingHashMap<>();

    /**
     * 批量添加规则链
     *
     * @param definitions 规则链定义list
     */
    public void addRuleChains(List<RuleChainDefinition> definitions) {
        for (RuleChainDefinition definition : definitions) {
            addRuleChain(definition);
        }
    }

    /**
     * 添加规则链
     *
     * @param definition 规则链定义
     */
    public void addRuleChain(RuleChainDefinition definition) {
        updateRuleChain0(definition, false);
    }

    /**
     * 更新规则链
     *
     * @param definition 规则链定义
     * @param overwrite  是否允许覆盖
     */
    private void updateRuleChain0(RuleChainDefinition definition, boolean overwrite) {
        List<RuleDefinition> childDefinitions = definition.getChain();
        int size = childDefinitions.size();
        RuleNode nextNode = parseDefinition(childDefinitions.get(size - 1), null);
        RuleNode curNode = null;
        for (int i = size - 2; i > 0; i--) {
            curNode = parseDefinition(childDefinitions.get(i), nextNode);
            nextNode = curNode;
        }
        String name = definition.getName();
        if (Objects.nonNull(ruleChains.put(name, new RuleChain(name, definition.getDesc(), curNode))) &&
                !overwrite) {
            throw new IllegalStateException(String.format("rule chain name '%s' conflict!!", name));
        }
    }

    /**
     * 解析规则定义, 并转换成{@link  RuleNode}实例
     *
     * @return {@link  RuleNode}实例
     */
    private RuleNode parseDefinition(RuleDefinition definition, RuleNode next) {
        switch (definition.getType()) {
            case PREDICATE:
                return new PredicateRule(definition, next);
            case SCRIPT:
                return new ScriptRule(definition, next);
            case TOPIC:
                return new TopicRule(definition, next);
            case HTTP:
                return new BridgeRule(definition, next);
            case KAFKA:
                return new BridgeRule(definition, next);
            case RABBITMQ:
                return new BridgeRule(definition, next);
            default:
                throw new IllegalArgumentException("unable to parse rule definition " + definition);
        }
    }

    /**
     * 更新规则链
     *
     * @param definition 规则链定义
     */
    public void updateRuleChain(RuleChainDefinition definition) {
        updateRuleChain0(definition, true);
    }

    /**
     * 批量更新规则链
     *
     * @param definitions 规则链定义list
     */
    public void updateRuleChains(List<RuleChainDefinition> definitions) {
        for (RuleChainDefinition definition : definitions) {
            updateRuleChain(definition);
        }
    }

    /**
     * 批量移除规则链
     *
     * @param chainNames 规则链唯一命名集合
     */
    public void removeRuleChains(Collection<String> chainNames) {
        for (String chainName : chainNames) {
            ruleChains.remove(chainName);
        }
    }

    /**
     * 批量移除规则链
     *
     * @param chainNames 规则链唯一命名集合
     */
    public void removeRuleChains(String... chainNames) {
        removeRuleChains(Arrays.asList(chainNames));
    }

    /**
     * 根据名字匹配规则链
     *
     * @param name 规则链名字
     * @return {@link RuleChain}
     */
    @Nullable
    public RuleChain getRuleChain(String name) {
        return ruleChains.get(name);
    }

    /**
     * @return 所有规则链
     */
    public Collection<RuleChain> getRuleChains() {
        return ruleChains.values();
    }
}
