package org.kin.mqtt.broker.bridge;

import org.kin.framework.Closeable;
import org.kin.framework.collection.CopyOnWriteMap;
import org.kin.framework.collection.Tuple;
import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.definition.BridgeDefinition;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.cluster.ClusterStore;
import org.kin.mqtt.broker.core.cluster.ClusterStoreKeys;
import org.kin.mqtt.broker.core.cluster.event.BridgeAddEvent;
import org.kin.mqtt.broker.core.cluster.event.BridgeChangedEvent;
import org.kin.mqtt.broker.core.cluster.event.BridgeRemoveEvent;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.kin.mqtt.broker.rule.Rule;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/12/11
 */
public class BridgeManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(BridgeManager.class);

    /** mqtt broker context */
    private final MqttBrokerContext brokerContext;
    /** key -> bridge name, value -> {@link Bridge} */
    private final Map<String, BridgeContext> bridgeMap = new CopyOnWriteMap<>();

    public BridgeManager(MqttBrokerContext brokerContext) {
        this.brokerContext = brokerContext;
    }

    /**
     * 初始化
     * cluster store初始化后开始
     * 1. 访问cluster store持久化配置
     * 2. 对比当前bridge与启动bridge配置, 若有变化, 则更新
     */
    public void init(List<BridgeDefinition> bridgeDefinitions) {
        //配置的bridge
        Map<String, BridgeDefinition> cName2Definition = bridgeDefinitions.stream().collect(Collectors.toMap(BridgeDefinition::getName, bd -> bd));
        //异步加载
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.scanRaw(ClusterStoreKeys.BRIDGE_KEY_PREFIX)
                .doOnNext(t -> onLoadFromClusterStore(t, cName2Definition))
                .doOnComplete(() -> onFinishLoadFromClusterStore(cName2Definition))
                .subscribe(v -> log.info("init bridge manager finished"),
                        t -> log.error("init bridge manager error", t));

        //注册内部consumer
        ReactorEventBus eventBus = brokerContext.getEventBus();
        eventBus.register(new BridgeAddEventConsumer());
        eventBus.register(new BridgeChangedEventConsumer());
        eventBus.register(new BridgeRemoveEventConsumer());
    }

    /**
     * 从cluster store加载到rule后, 将{@link RuleDefinition}转换成{@link Rule}实例
     */
    private void onLoadFromClusterStore(Tuple<String, byte[]> tuple, Map<String, BridgeDefinition> cName2Definition) {
        String key = tuple.first();
        if (!ClusterStoreKeys.isBridgeKey(key)) {
            //过滤非法key
            return;
        }

        //持久化bridge配置
        BridgeDefinition definition = toBridgeDefinition(tuple.second());
        String name = definition.getName();
        //新bridge配置
        BridgeDefinition cDefinition = cName2Definition.get(name);
        //最终配置
        BridgeDefinition fDefinition;
        if (cDefinition == null) {
            //应用持久化bridge配置
            fDefinition = definition;
        }
        else{
            //应用新bridge配置
            fDefinition = cDefinition;
        }

        Bridge bridge = Bridges.createBridge(fDefinition);
        bridgeMap.put(name, new BridgeContext(fDefinition, bridge));
        if (cDefinition != definition) {
            //新配置, 需更新db中的bridge配置
            persistDefinition(fDefinition);
            brokerContext.broadcastClusterEvent(BridgeAddEvent.of(name));
        }
    }

    /**
     * 从cluster store加载bridge完成后, 把{@code cName2Definition}中有的, {@link #bridgeMap}中没有的加载
     */
    private void onFinishLoadFromClusterStore(Map<String, BridgeDefinition> cName2Definition) {
        for (BridgeDefinition definition : cName2Definition.values()) {
            String name = definition.getName();
            if (bridgeMap.containsKey(name)) {
                continue;
            }

            addBridge0(definition);
        }
    }

    /**
     * 添加数据桥接
     *
     * @param definition 桥接配置
     */
    public void addBridge(BridgeDefinition definition) {
        definition.check();

        String bridgeName = definition.getName();
        if (bridgeMap.containsKey(bridgeName)) {
            throw new IllegalArgumentException(String.format("bridge name '%s' conflict!!", bridgeName));
        }
        addBridge0(definition);
    }

    private void addBridge0(BridgeDefinition definition) {
        String bridgeName = definition.getName();

        Bridge bridge = Bridges.createBridge(definition);
        bridgeMap.put(bridgeName, new BridgeContext(definition, bridge));
        persistDefinition(definition);
        brokerContext.broadcastClusterEvent(BridgeAddEvent.of(bridgeName));
    }

    /**
     * 更新数据桥接
     *
     * @param nDefinition 新桥接配置
     */
    public void updateBridge(BridgeDefinition nDefinition) {
        nDefinition.check();

        String bridgeName = nDefinition.getName();
        BridgeContext bridgeContext = bridgeMap.get(bridgeName);
        if (Objects.isNull(bridgeContext)) {
            throw new IllegalArgumentException(String.format("can not find registered bridge named '%s'", bridgeName));
        }

        if(nDefinition.equals(bridgeContext.getDefinition())){
            throw new IllegalArgumentException("bridge definition is complete same, " + nDefinition);
        }

        //old bridge close
        bridgeContext.close();

        Bridge bridge = Bridges.createBridge(nDefinition);
        bridgeMap.put(bridgeName, new BridgeContext(nDefinition, bridge));
        persistDefinition(nDefinition);
        brokerContext.broadcastClusterEvent(BridgeAddEvent.of(bridgeName));
    }

    /**
     * 移除数据桥接实现
     *
     * @param bridgeName 数据桥接名
     * @return 是否移除成功
     */
    public boolean removeBridge(String bridgeName) {
        Bridge removed = bridgeMap.remove(bridgeName);
        if (Objects.nonNull(removed)) {
            brokerContext.broadcastClusterEvent(BridgeRemoveEvent.of(bridgeName));
            removed.close();
        }
        return Objects.nonNull(removed);
    }

    /**
     * 持久化bridge配置
     *
     * @param definition bridge配置
     */
    private void persistDefinition(BridgeDefinition definition) {
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.put(ClusterStoreKeys.getBridgeKey(definition.getName()), new BridgeDefinitionDelegate(definition))
                .subscribe();
    }

    /**
     * 将序列化后的字节数组转换为{@link BridgeDefinition}实例
     *
     * @param bytes 序列化后的字节数组
     * @return {@link BridgeDefinition}实例
     */
    private BridgeDefinition toBridgeDefinition(byte[] bytes) {
        return ((BridgeDefinitionDelegate) JSON.read(bytes, BridgeDefinitionDelegate.class)).getDelegate();
    }

    /**
     * 同步桥接变化
     *
     * @param name 规则名
     */
    private void syncBridge(String name) {
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.get(ClusterStoreKeys.getRuleKey(name), BridgeDefinitionDelegate.class)
                .doOnNext(bdd -> syncBridge0(bdd.getDelegate()))
                .subscribe(v -> log.error("sync bridge '{}' finished", name),
                        t -> log.error("sync bridge '{}' error", name, t));
    }

    /**
     * 同步桥接变化
     * @param definition    桥接配置
     */
    private void syncBridge0(BridgeDefinition definition){
        definition.check();

        String bridgeName = definition.getName();
        BridgeContext bridgeContext = bridgeMap.get(bridgeName);
        if (Objects.nonNull(bridgeContext)) {
            //old bridge close
            bridgeContext.close();
        }

        Bridge bridge = Bridges.createBridge(definition);
        bridgeMap.put(bridgeName, new BridgeContext(definition, bridge));
    }

    /**
     * 根据数据桥接名字获取{@link  Bridge}实例
     *
     * @param bridgeName 桥接名字
     * @return {@link  Bridge}实例
     */
    @Nullable
    public Bridge getBridge(String bridgeName) {
        return bridgeMap.get(bridgeName);
    }

    @Override
    public void close() {
        for (Bridge bridge : bridgeMap.values()) {
            bridge.close();
        }
    }

    //--------------------------------------------------------internal mqtt event consumer

    /**
     * 新增数据桥接事件consumer
     */
    private class BridgeAddEventConsumer implements MqttEventConsumer<BridgeAddEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, BridgeAddEvent event) {
            syncBridge(event.getBridgeName());
        }
    }

    /**
     * 数据桥接更新事件consumer
     */
    private class BridgeChangedEventConsumer implements MqttEventConsumer<BridgeChangedEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, BridgeChangedEvent event) {
            syncBridge(event.getBridgeName());
        }
    }

    /**
     * 移除数据桥接事件consumer
     */
    private class BridgeRemoveEventConsumer implements MqttEventConsumer<BridgeRemoveEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, BridgeRemoveEvent event) {
            syncBridge(event.getBridgeName());
        }
    }
}
