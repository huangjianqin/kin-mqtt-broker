package org.kin.mqtt.broker.rule;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.collection.Tuple;
import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.cluster.ClusterStore;
import org.kin.mqtt.broker.core.cluster.ClusterStoreKeys;
import org.kin.mqtt.broker.core.cluster.event.RuleAddEvent;
import org.kin.mqtt.broker.core.cluster.event.RuleChangedEvent;
import org.kin.mqtt.broker.core.cluster.event.RuleRemoveEvent;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.kin.mqtt.broker.rule.action.ActionDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 规则定义管理
 * @author huangjianqin
 * @date 2022/11/21
 */
public class RuleManager {
    private static final Logger log = LoggerFactory.getLogger(RuleManager.class);

    /** broker context */
    private final MqttBrokerContext brokerContext;
    /** key -> rule name, value -> {@link Rule} */
    private final Map<String, Rule> rules = new NonBlockingHashMap<>();

    public RuleManager(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
    }

    /**
     * 初始化
     * cluster store初始化后开始
     * 1. 访问cluster store持久化配置
     * 2. 对比当前rule与启动rule配置, 若有变化, 则更新
     */
    public void init(List<RuleDefinition> ruleDefinitions){
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.scanRaw(ClusterStoreKeys.RULE_KEY_PREFIX)
                .doOnNext(this::onLoadFromClusterStore)
                .doOnComplete(() -> onFinishLoadFromClusterStore(ruleDefinitions))
                .subscribe(null, t -> log.error("init rule manager error", t));

        //注册内部consumer
        ReactorEventBus eventBus = brokerContext.getEventBus();
        eventBus.register(new RuleAddEventConsumer());
        eventBus.register(new RuleChangedEventConsumer());
        eventBus.register(new RuleRemoveEventConsumer());
    }

    /**
     * 从cluster store加载到rule后, 将{@link RuleDefinition}转换成{@link Rule}实例
     */
    private void onLoadFromClusterStore(Tuple<String, byte[]> tuple){
        String key = tuple.first();
        if(!ClusterStoreKeys.isRuleKey(key)){
            return;
        }

        RuleDefinition definition = JSON.read(tuple.second(), RuleDefinition.class);
        rules.put(definition.getName(), new Rule(definition));
    }

    /**
     * 从cluster store加载rule完成后, 对比启动rule配置, 若有变化, 则更新rule
     */
    private void onFinishLoadFromClusterStore(List<RuleDefinition> ruleDefinitions){
        for (RuleDefinition definition : ruleDefinitions) {
            String name = definition.getName();
            try {
                log.debug("add or merge rule '{}'...", name);
                addOrMergeRule(definition, true);
            } catch (Exception e) {
                log.error("add or merge rule '{}' error", name, e);
            }
        }
    }

    /**
     * 添加或覆盖规则
     *
     * @param definition 规则定义
     * @param clusterSync  是否需要集群间同步
     */
    private void addOrMergeRule(RuleDefinition definition, boolean clusterSync) {
        definition.check();

        String name = definition.getName();
        Rule oldRule = rules.get(name);
        if (oldRule!=null && oldRule.getDefinition().equals(definition)) {
            //rule没有变化
            log.debug("add or merge rule '{}' fail, due to rule has no changed", name);
            return;
        }

        rules.put(name, new Rule(definition));
        persistRuleDefinition(definition);
        if (oldRule != null) {
            oldRule.dispose();
        }
        log.debug("add or merge rule '{}' success", name);

        if(clusterSync){
            brokerContext.broadcastClusterEvent(RuleAddEvent.of(definition.getName()));
        }
    }

    /**
     * 批量添加规则
     *
     * @param definitions 规则定义list
     */
    public void addRules(Collection<RuleDefinition> definitions) {
        for (RuleDefinition definition : definitions) {
            addRule(definition);
        }
    }

    /**
     * 批量添加规则
     *
     * @param definitions 规则定义array
     */
    public void addRules(RuleDefinition... definitions) {
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
        definition.check();

        String name = definition.getName();
        if (rules.containsKey(name)) {
            throw new IllegalStateException(String.format("rule '%s' has registered", name));
        }

        rules.put(name, new Rule(definition));
        persistRuleDefinition(definition);

        log.debug("add rule '{}' success", name);

        brokerContext.broadcastClusterEvent(RuleAddEvent.of(definition.getName()));
    }

    /**
     * 持久化规则定义
     * @param definition 规则定义
     */
    private void persistRuleDefinition(RuleDefinition definition){
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.put(ClusterStoreKeys.getRuleKey(definition.getName()), definition)
                .subscribe();
    }

    /**
     * 移除规则, 如果没有则抛异常
     *
     * @param name 规则名
     */
    public void removeRule(String name) {
        Rule removed = rules.remove(name);
        if (Objects.isNull(removed)) {
            throw new IllegalStateException(String.format("can not find rule '%s'", name));
        }

        removed.dispose();
        delRuleDefinition(removed.getDefinition());

        log.debug("remove rule '{}' success", name);

        brokerContext.broadcastClusterEvent(RuleRemoveEvent.of(name));
    }

    /**
     * 移除持久化规则定义
     * @param definition 规则定义
     */
    private void delRuleDefinition(RuleDefinition definition){
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.delete(ClusterStoreKeys.getRuleKey(definition.getName()))
                .subscribe();
    }

    /**
     * 获取规则, 如果没有则抛异常
     *
     * @param name 规则名
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
     * 同步规则变化
     * @param name  规则名
     */
    private void syncRule(String name){
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.get(ClusterStoreKeys.getRuleKey(name), RuleDefinition.class)
                .doOnNext(rd -> addOrMergeRule(rd, false))
                .subscribe(null, t -> log.error("sync rule '{}' error", name, t));
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
        persistRuleDefinition(rule.getDefinition());

        log.debug("add action to rule '{}' success, {}", name, actionDefinition);

        brokerContext.broadcastClusterEvent(RuleChangedEvent.of(name));
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
            persistRuleDefinition(rule.getDefinition());
            log.debug("remove action from rule '{}' success, {}", name, actionDefinition);
            brokerContext.broadcastClusterEvent(RuleChangedEvent.of(name));
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

    //--------------------------------------------------------internal mqtt event consumer

    /**
     * 新增规则事件consumer
     */
    private class RuleAddEventConsumer implements MqttEventConsumer<RuleAddEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, RuleAddEvent event) {
            syncRule(event.getRuleName());
        }
    }

    /**
     * 规则变更事件consumer
     */
    private class RuleChangedEventConsumer implements MqttEventConsumer<RuleChangedEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, RuleChangedEvent event) {
            syncRule(event.getRuleName());
        }
    }

    /**
     * 移除规则事件consumer
     */
    private class RuleRemoveEventConsumer implements MqttEventConsumer<RuleRemoveEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, RuleRemoveEvent event) {
            syncRule(event.getRuleName());
        }
    }
}
