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
import org.kin.mqtt.broker.rule.action.ActionConfiguration;
import org.kin.mqtt.broker.rule.event.RuleAddEvent;
import org.kin.mqtt.broker.rule.event.RuleChangedEvent;
import org.kin.mqtt.broker.rule.event.RuleRemoveEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 规则管理
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
    public void init(List<RuleConfiguration> ruleConfigs) {
        //配置的bridge
        Map<String, RuleConfiguration> cName2Config = ruleConfigs.stream().collect(Collectors.toMap(RuleConfiguration::getName, d -> d));
        //异步加载
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.scanRaw(ClusterStoreKeys.RULE_KEY_PREFIX)
                .doOnNext(t -> onLoadFromClusterStore(t, cName2Config))
                .doOnComplete(() -> onFinishLoadFromClusterStore(ruleConfigs))
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
    private void onLoadFromClusterStore(Tuple<String, byte[]> tuple, Map<String, RuleConfiguration> cName2Config) {
        String key = tuple.first();
        if(!ClusterStoreKeys.isRuleKey(key)){
            //过滤非法key
            return;
        }

        byte[] bytes = tuple.second();
        if (Objects.isNull(bytes)){
            return;
        }

        //持久化rule配置
        RuleConfiguration pConfig = JSON.read(bytes, RuleConfiguration.class);
        String name = pConfig.getName();
        //新rule配置
        RuleConfiguration nConfig = cName2Config.get(name);
        //最终配置
        RuleConfiguration fConfig;
        if (nConfig == null) {
            //应用持久化bridge配置
            fConfig = pConfig;
        } else {
            //应用新bridge配置
            fConfig = nConfig;
        }

        ruleMap.put(name, new Rule(fConfig));
        if (!fConfig.equals(pConfig)) {
            //新配置, 需更新db中的rule配置
            persistConfig(fConfig);
            brokerContext.broadcastClusterEvent(RuleAddEvent.of(name));
        }
    }

    /**
     * 从cluster store加载rule完成后, 把{@code ruleConfigs}中有的, {@link #ruleMap}中没有的rule配置加载
     */
    private void onFinishLoadFromClusterStore(List<RuleConfiguration> ruleConfigs) {
        List<String> newRuleNames = new ArrayList<>(ruleConfigs.size());
        for (RuleConfiguration ruleConfig : ruleConfigs) {
            String name = ruleConfig.getName();
            if (ruleMap.containsKey(name)) {
                continue;
            }

            newRuleNames.add(name);
            addRule1(ruleConfig);
        }
        if (CollectionUtils.isNonEmpty(newRuleNames)) {
            brokerContext.broadcastClusterEvent(RuleAddEvent.of(newRuleNames));
        }
    }

    /**
     * 批量添加规则
     *
     * @param ruleConfigs 规则配置list
     */
    public void addRules(Collection<RuleConfiguration> ruleConfigs) {
        List<String> names = new ArrayList<>(ruleConfigs.size());
        try {
            for (RuleConfiguration ruleConfig : ruleConfigs) {
                addRule0(ruleConfig);
                names.add(ruleConfig.getName());
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
     * @param ruleConfigs 规则配置array
     */
    public void addRules(RuleConfiguration... ruleConfigs) {
        List<String> names = new ArrayList<>(ruleConfigs.length);
        try {
            for (RuleConfiguration ruleConfig : ruleConfigs) {
                addRule0(ruleConfig);
                names.add(ruleConfig.getName());
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
     * @param ruleConfig 规则配置
     */
    public void addRule(RuleConfiguration ruleConfig) {
        addRule0(ruleConfig);
        brokerContext.broadcastClusterEvent(RuleAddEvent.of(ruleConfig.getName()));
    }

    private void addRule0(RuleConfiguration ruleConfig) {
        ruleConfig.check();

        String name = ruleConfig.getName();
        if (ruleMap.containsKey(name)) {
            throw new IllegalArgumentException(String.format("rule '%s' has registered", name));
        }

        addRule1(ruleConfig);
    }

    private void addRule1(RuleConfiguration ruleConfig) {
        String name = ruleConfig.getName();

        ruleMap.put(name, new Rule(ruleConfig));
        persistConfig(ruleConfig);

        log.debug("add rule '{}' success", name);
    }

    /**
     * 更新规则
     *
     * @param nRuleConfig 新规则配置
     */
    public void updateRule(RuleConfiguration nRuleConfig) {
        nRuleConfig.check();

        String name = nRuleConfig.getName();
        Rule oldRule = ruleMap.get(name);
        if (Objects.isNull(oldRule)) {
            throw new IllegalArgumentException(String.format("rule '%s' has registered", name));
        }

        if (nRuleConfig.equals(oldRule.getConfig())) {
            throw new IllegalArgumentException("rule configuration is complete same, " + nRuleConfig);
        }

        //dispose old rule
        oldRule.dispose();

        ruleMap.put(name, new Rule(nRuleConfig));
        persistConfig(nRuleConfig);

        log.debug("add rule '{}' success", name);

        brokerContext.broadcastClusterEvent(RuleAddEvent.of(name));
    }

    /**
     * 持久化规则配置
     * @param ruleConfig 规则配置
     */
    private void persistConfig(RuleConfiguration ruleConfig) {
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.put(ClusterStoreKeys.getRuleKey(ruleConfig.getName()), ruleConfig)
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
        delConfig(removed.getConfig());

        log.debug("remove rule '{}' success", name);

        brokerContext.broadcastClusterEvent(RuleRemoveEvent.of(name));
    }

    /**
     * 移除持久化规则配置
     *
     * @param ruleConfig 规则配置
     */
    private void delConfig(RuleConfiguration ruleConfig) {
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.delete(ClusterStoreKeys.getRuleKey(ruleConfig.getName()))
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
        clusterStore.multiGet(keys, RuleConfiguration.class)
                .doOnNext(t -> syncRule(t.second()))
                .subscribe(null,
                        t -> log.error("sync rule '{}' error", desc, t),
                        () -> log.error("sync rule '{}' finished", desc));
    }

    /**
     * 集群广播rule变化, 本broker收到事件通知后, 从cluster store加载rule configuration并apply
     *
     * @param ruleConfig rule配置
     */
    private void syncRule(@Nullable RuleConfiguration ruleConfig) {
        if (Objects.isNull(ruleConfig)) {
            return;
        }
        ruleConfig.check();

        String name = ruleConfig.getName();
        Rule oldRule = ruleMap.get(name);
        if (Objects.nonNull(oldRule)) {
            //old rule dispose
            oldRule.dispose();
        }

        ruleMap.put(name, new Rule(ruleConfig));
        persistConfig(ruleConfig);
    }

    /**
     * 添加动作
     *
     * @param name             规则名字
     * @param actionConfig 动作配置
     */
    public void addAction(String name, ActionConfiguration actionConfig) {
        Rule rule = getRuleOrThrow(name);
        rule.addAction(actionConfig);
        persistConfig(rule.getConfig());

        log.debug("add action to rule '{}' success, {}", name, actionConfig);

        brokerContext.broadcastClusterEvent(RuleChangedEvent.of(name));
    }

    /**
     * 移除动作
     *
     * @param name             规则名字
     * @param actionConfig 动作配置
     */
    public boolean removeAction(String name, ActionConfiguration actionConfig) {
        Rule rule = getRuleOrThrow(name);
        boolean result = rule.removeAction(actionConfig);
        if (result) {
            persistConfig(rule.getConfig());
            log.debug("remove action from rule '{}' success, {}", name, actionConfig);
            brokerContext.broadcastClusterEvent(RuleChangedEvent.of(name));
        }
        return result;
    }

    /**
     * 根据名字匹配规则配置
     *
     * @param name 规则名字
     * @return 规则配置
     */
    @Nullable
    public RuleConfiguration getRuleConfiguration(String name) {
        Rule rule = ruleMap.get(name);
        if (Objects.nonNull(rule)) {
            return rule.getConfig();
        }
        return null;
    }

    /**
     * 获取所有规则配置
     *
     * @return 所有规则配置
     */
    public Collection<RuleConfiguration> getAllRuleConfigurations() {
        return ruleMap.values().stream().map(Rule::getConfig).collect(Collectors.toList());
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
