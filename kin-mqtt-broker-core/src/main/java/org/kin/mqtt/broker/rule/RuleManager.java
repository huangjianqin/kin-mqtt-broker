package org.kin.mqtt.broker.rule;

import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.collection.Tuple;
import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.cluster.ClusterStore;
import org.kin.mqtt.broker.core.cluster.ClusterStoreKeys;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.kin.mqtt.broker.rule.action.ActionDefinition;
import org.kin.mqtt.broker.rule.event.RuleAddEvent;
import org.kin.mqtt.broker.rule.event.RuleChangedEvent;
import org.kin.mqtt.broker.rule.event.RuleRemoveEvent;
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
    private final Map<String, Rule> ruleMap = new NonBlockingHashMap<>();

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
        //配置的bridge
        Map<String, RuleDefinition> cName2Definition = ruleDefinitions.stream().collect(Collectors.toMap(RuleDefinition::getName, d -> d));
        //异步加载
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.scanRaw(ClusterStoreKeys.RULE_KEY_PREFIX)
                .doOnNext(t -> onLoadFromClusterStore(t, cName2Definition))
                .doOnComplete(() -> onFinishLoadFromClusterStore(ruleDefinitions))
                .subscribe(null,
                        t -> log.error("init rule manager error", t),
                        () -> log.info("init rule manager finished"));

        //注册内部consumer
        ReactorEventBus eventBus = brokerContext.getEventBus();
        eventBus.register(new RuleAddEventConsumer());
        eventBus.register(new RuleChangedEventConsumer());
        eventBus.register(new RuleRemoveEventConsumer());
    }

    /**
     * 从cluster store加载到rule后, 对比启动rule配置, 若有变化, 则存储新rule, 并广播集群其余broker节点
     */
    private void onLoadFromClusterStore(Tuple<String, byte[]> tuple, Map<String, RuleDefinition> cName2Definition){
        String key = tuple.first();
        if(!ClusterStoreKeys.isRuleKey(key)){
            //过滤非法key
            return;
        }

        //持久化rule配置
        RuleDefinition definition = JSON.read(tuple.second(), RuleDefinition.class);
        String name = definition.getName();
        //新rule配置
        RuleDefinition cDefinition = cName2Definition.get(name);
        //最终配置
        RuleDefinition fDefinition;
        if (cDefinition == null) {
            //应用持久化bridge配置
            fDefinition = definition;
        } else {
            //应用新bridge配置
            fDefinition = cDefinition;
        }

        ruleMap.put(name, new Rule(fDefinition));
        if (!fDefinition.equals(definition)) {
            //新配置, 需更新db中的rule配置
            persistDefinition(fDefinition);
            brokerContext.broadcastClusterEvent(RuleAddEvent.of(name));
        }
    }

    /**
     * 从cluster store加载rule完成后, 把{@code ruleDefinitions}中有的, {@link #ruleMap}中没有的rule配置加载
     */
    private void onFinishLoadFromClusterStore(List<RuleDefinition> ruleDefinitions){
        List<String> newRuleNames = new ArrayList<>(ruleDefinitions.size());
        for (RuleDefinition definition : ruleDefinitions) {
            String name = definition.getName();
            if (ruleMap.containsKey(name)) {
                continue;
            }

            newRuleNames.add(name);
            addRule1(definition);
        }
        if (CollectionUtils.isNonEmpty(newRuleNames)) {
            brokerContext.broadcastClusterEvent(RuleAddEvent.of(newRuleNames));
        }
    }

    /**
     * 批量添加规则
     *
     * @param definitions 规则定义list
     */
    public void addRules(Collection<RuleDefinition> definitions) {
        List<String> names = new ArrayList<>(definitions.size());
        try {
            for (RuleDefinition definition : definitions) {
                addRule0(definition);
                names.add(definition.getName());
            }
        } finally {
            if (CollectionUtils.isNonEmpty(names)) {
                brokerContext.broadcastClusterEvent(RuleAddEvent.of(names));
            }
        }
    }

    /**
     * 批量添加规则
     *
     * @param definitions 规则定义array
     */
    public void addRules(RuleDefinition... definitions) {
        List<String> names = new ArrayList<>(definitions.length);
        try {
            for (RuleDefinition definition : definitions) {
                addRule0(definition);
                names.add(definition.getName());
            }
        } finally {
            if (CollectionUtils.isNonEmpty(names)) {
                brokerContext.broadcastClusterEvent(RuleAddEvent.of(names));
            }
        }
    }

    /**
     * 添加规则
     *
     * @param definition 规则定义
     */
    public void addRule(RuleDefinition definition) {
        addRule0(definition);
        brokerContext.broadcastClusterEvent(RuleAddEvent.of(definition.getName()));
    }

    private void addRule0(RuleDefinition definition) {
        definition.check();

        String name = definition.getName();
        if (ruleMap.containsKey(name)) {
            throw new IllegalArgumentException(String.format("rule '%s' has registered", name));
        }

        addRule1(definition);
    }

    private void addRule1(RuleDefinition definition) {
        String name = definition.getName();

        ruleMap.put(name, new Rule(definition));
        persistDefinition(definition);

        log.debug("add rule '{}' success", name);
    }

    /**
     * 更新规则
     *
     * @param nDefinition 新规则定义
     */
    public void updateRule(RuleDefinition nDefinition) {
        nDefinition.check();

        String name = nDefinition.getName();
        Rule oldRule = ruleMap.get(name);
        if (Objects.isNull(oldRule)) {
            throw new IllegalArgumentException(String.format("rule '%s' has registered", name));
        }

        if (nDefinition.equals(oldRule.getDefinition())) {
            throw new IllegalArgumentException("rule definition is complete same, " + nDefinition);
        }

        //dispose old rule
        oldRule.dispose();

        ruleMap.put(name, new Rule(nDefinition));
        persistDefinition(nDefinition);

        log.debug("add rule '{}' success", name);

        brokerContext.broadcastClusterEvent(RuleAddEvent.of(name));
    }

    /**
     * 持久化规则定义
     * @param definition 规则定义
     */
    private void persistDefinition(RuleDefinition definition){
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
        Rule removed = ruleMap.remove(name);
        if (Objects.isNull(removed)) {
            throw new IllegalStateException(String.format("can not find rule '%s'", name));
        }

        removed.dispose();
        delDefinition(removed.getDefinition());

        log.debug("remove rule '{}' success", name);

        brokerContext.broadcastClusterEvent(RuleRemoveEvent.of(name));
    }

    /**
     * 移除持久化规则定义
     *
     * @param definition 规则定义
     */
    private void delDefinition(RuleDefinition definition) {
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
        Rule rule = ruleMap.get(name);
        if (Objects.isNull(rule)) {
            throw new IllegalStateException(String.format("can not find rule '%s'", name));
        }

        return rule;
    }

    /**
     * 同步规则变化
     *
     * @param names 规则名数组
     */
    private void syncRules(List<String> names) {
        if (CollectionUtils.isEmpty(names)) {
            return;
        }

        List<String> keys = names.stream().map(ClusterStoreKeys::getRuleKey).collect(Collectors.toList());
        String desc = StringUtils.mkString(names);

        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.multiGet(keys, RuleDefinition.class)
                .doOnNext(t -> syncRule(t.second()))
                .subscribe(null,
                        t -> log.error("sync rule '{}' error", desc, t),
                        () -> log.error("sync rule '{}' finished", desc));
    }

    /**
     * 集群广播rule变化, 本broker收到事件通知后, 从cluster store加载rule definition并apply
     *
     * @param definition rule配置
     */
    private void syncRule(RuleDefinition definition) {
        definition.check();

        String name = definition.getName();
        Rule oldRule = ruleMap.get(name);
        if (Objects.nonNull(oldRule)) {
            //old rule dispose
            oldRule.dispose();
        }

        ruleMap.put(name, new Rule(definition));
        persistDefinition(definition);
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
        persistDefinition(rule.getDefinition());

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
            persistDefinition(rule.getDefinition());
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
        Rule rule = ruleMap.get(name);
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
    public Collection<RuleDefinition> getAllRuleDefinitions() {
        return ruleMap.values().stream().map(Rule::getDefinition).collect(Collectors.toList());
    }

    /**
     * 获取所有规则实现
     *
     * @return 所有规则实现
     */
    public Collection<Rule> getAllRule() {
        return new ArrayList<>(ruleMap.values());
    }

    //--------------------------------------------------------internal mqtt event consumer

    /**
     * 新增规则事件consumer
     */
    private class RuleAddEventConsumer implements MqttEventConsumer<RuleAddEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, RuleAddEvent event) {
            syncRules(event.getRuleNames());
        }
    }

    /**
     * 规则变更事件consumer
     */
    private class RuleChangedEventConsumer implements MqttEventConsumer<RuleChangedEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, RuleChangedEvent event) {
            syncRules(event.getRuleNames());
        }
    }

    /**
     * 移除规则事件consumer
     */
    private class RuleRemoveEventConsumer implements MqttEventConsumer<RuleRemoveEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, RuleRemoveEvent event) {
            syncRules(event.getRuleNames());
        }
    }
}
