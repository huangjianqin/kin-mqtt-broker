package org.kin.mqtt.broker.bridge;

import org.kin.framework.Closeable;
import org.kin.framework.collection.CopyOnWriteMap;
import org.kin.framework.collection.Tuple;
import org.kin.framework.reactor.event.ReactorEventBus;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.ExtensionLoader;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.bridge.event.BridgeAddEvent;
import org.kin.mqtt.broker.bridge.event.BridgeChangedEvent;
import org.kin.mqtt.broker.bridge.event.BridgeRemoveEvent;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.cluster.ClusterStore;
import org.kin.mqtt.broker.core.cluster.ClusterStoreKeys;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
    public void init(List<BridgeConfiguration> configs) {
        //配置的bridge
        Map<String, BridgeConfiguration> cName2Config = configs.stream().collect(Collectors.toMap(BridgeConfiguration::getName, bd -> bd));
        //异步加载
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.scanRaw(ClusterStoreKeys.BRIDGE_KEY_PREFIX)
                .doOnNext(t -> onLoadFromClusterStore(t, cName2Config))
                .doOnComplete(() -> onFinishLoadFromClusterStore(cName2Config))
                .subscribe(null,
                        t -> log.error("init bridge manager error", t),
                        () -> log.info("init bridge manager finished"));

        //注册内部consumer
        ReactorEventBus eventBus = brokerContext.getEventBus();
        eventBus.register(new BridgeAddEventConsumer());
        eventBus.register(new BridgeChangedEventConsumer());
        eventBus.register(new BridgeRemoveEventConsumer());
    }

    /**
     * 从cluster store加载到rule后, 对比启动bridge配置, 若有变化, 则存储新bridge, 并广播集群其余broker节点
     */
    private void onLoadFromClusterStore(Tuple<String, byte[]> tuple, Map<String, BridgeConfiguration> cName2Config) {
        String key = tuple.first();
        if (!ClusterStoreKeys.isBridgeKey(key)) {
            //过滤非法key
            return;
        }

        byte[] bytes = tuple.second();
        if (Objects.isNull(bytes)) {
            return;
        }

        //持久化bridge配置
        BridgeConfiguration pConfig = toBridgeConfig(bytes);
        String name = pConfig.getName();
        //新bridge配置
        BridgeConfiguration nConfig = cName2Config.get(name);
        //最终配置
        BridgeConfiguration fConfig;
        if (nConfig == null) {
            //应用持久化bridge配置
            fConfig = pConfig;
        } else {
            //应用新bridge配置
            fConfig = nConfig;
        }

        Bridge bridge = createBridge(fConfig);
        bridgeMap.put(name, new BridgeContext(fConfig, bridge));
        if (!fConfig.equals(pConfig)) {
            //新配置, 需更新db中的bridge配置
            persistConfig(fConfig);
            brokerContext.broadcastClusterEvent(BridgeAddEvent.of(name));
        }
    }

    /**
     * 从cluster store加载bridge完成后, 把{@code name2Config}中有的, {@link #bridgeMap}中没有的bridge配置加载
     */
    private void onFinishLoadFromClusterStore(Map<String, BridgeConfiguration> name2Config) {
        List<String> newBridgeNames = new ArrayList<>(name2Config.size());
        for (BridgeConfiguration config : name2Config.values()) {
            String name = config.getName();
            if (bridgeMap.containsKey(name)) {
                continue;
            }

            newBridgeNames.add(name);
            addBridge0(config);
        }
        if (CollectionUtils.isNonEmpty(newBridgeNames)) {
            brokerContext.broadcastClusterEvent(BridgeAddEvent.of(newBridgeNames));
        }
    }

    /**
     * 创建{@link Bridge}实例
     *
     * @return {@link Bridge}实例
     */
    @SuppressWarnings("unchecked")
    private static Bridge createBridge(BridgeConfiguration config) {
        String type = config.getType();
        BridgeFactory<Bridge> factory = ExtensionLoader.getExtension(BridgeFactory.class, type);
        if (Objects.isNull(factory)) {
            throw new IllegalArgumentException(String.format("can not find bridge factory named '%s'", type));
        }

        return factory.create(config);
    }

    /**
     * 添加数据桥接
     *
     * @param config 桥接配置
     */
    public void addBridge(BridgeConfiguration config) {
        config.check();

        String bridgeName = config.getName();
        if (bridgeMap.containsKey(bridgeName)) {
            throw new IllegalArgumentException(String.format("bridge name '%s' conflict!!", bridgeName));
        }
        addBridge0(config);
        brokerContext.broadcastClusterEvent(BridgeAddEvent.of(bridgeName));
    }

    private void addBridge0(BridgeConfiguration config) {
        String bridgeName = config.getName();

        Bridge bridge = createBridge(config);
        bridgeMap.put(bridgeName, new BridgeContext(config, bridge));
        persistConfig(config);
    }

    /**
     * 更新数据桥接
     *
     * @param nConfig 新桥接配置
     */
    public void updateBridge(BridgeConfiguration nConfig) {
        nConfig.check();

        String bridgeName = nConfig.getName();
        BridgeContext bridgeContext = bridgeMap.get(bridgeName);
        if (Objects.isNull(bridgeContext)) {
            throw new IllegalArgumentException(String.format("can not find registered bridge named '%s'", bridgeName));
        }

        if (nConfig.equals(bridgeContext.getConfig())) {
            throw new IllegalArgumentException("bridge configuration is same completely, " + nConfig);
        }

        //old bridge close
        bridgeContext.close();

        Bridge bridge = createBridge(nConfig);
        bridgeMap.put(bridgeName, new BridgeContext(nConfig, bridge));
        persistConfig(nConfig);
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
            delConfig(bridgeName);

            brokerContext.broadcastClusterEvent(BridgeRemoveEvent.of(bridgeName));
            removed.close();
        }
        return Objects.nonNull(removed);
    }

    /**
     * 持久化bridge配置
     *
     * @param config bridge配置
     */
    private void persistConfig(BridgeConfiguration config) {
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.put(ClusterStoreKeys.getBridgeKey(config.getName()), config)
                .subscribe();
    }

    /**
     * 移除bridge配置
     *
     * @param bridgeName bridge name
     */
    private void delConfig(String bridgeName) {
        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.delete(ClusterStoreKeys.getBridgeKey(bridgeName))
                .subscribe();
    }

    /**
     * 将序列化后的字节数组转换为{@link BridgeConfiguration}实例
     *
     * @param bytes 序列化后的字节数组
     * @return {@link BridgeConfiguration}实例
     */
    private BridgeConfiguration toBridgeConfig(byte[] bytes) {
        return JSON.read(bytes, BridgeConfiguration.class);
    }

    /**
     * 同步桥接变化
     *
     * @param names 桥接名列表
     */
    private void syncBridges(List<String> names) {
        if (CollectionUtils.isEmpty(names)) {
            return;
        }

        String desc = StringUtils.mkString(names);
        List<String> keys = names.stream().map(ClusterStoreKeys::getBridgeKey).collect(Collectors.toList());

        ClusterStore clusterStore = brokerContext.getClusterStore();
        clusterStore.multiGet(keys, BridgeConfiguration.class)
                .doOnNext(t -> syncBridge(t.second()))
                .subscribe(null,
                        t -> log.error("sync bridge '{}' error", desc, t),
                        () -> log.error("sync bridge '{}' finished", desc));
    }

    /**
     * 集群广播bridge变化, 本broker收到事件通知后, 从cluster store加载bridge configuration并apply
     *
     * @param config 桥接配置
     */
    private void syncBridge(@Nullable BridgeConfiguration config) {
        if (Objects.isNull(config)) {
            return;
        }

        config.check();

        String bridgeName = config.getName();
        BridgeContext bridgeContext = bridgeMap.get(bridgeName);
        if (Objects.nonNull(bridgeContext)) {
            //old bridge close
            bridgeContext.close();
        }

        Bridge bridge = createBridge(config);
        bridgeMap.put(bridgeName, new BridgeContext(config, bridge));
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
            syncBridges(event.getBridgeNames());
        }
    }

    /**
     * 数据桥接更新事件consumer
     */
    private class BridgeChangedEventConsumer implements MqttEventConsumer<BridgeChangedEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, BridgeChangedEvent event) {
            syncBridges(event.getBridgeNames());
        }
    }

    /**
     * 移除数据桥接事件consumer
     */
    private class BridgeRemoveEventConsumer implements MqttEventConsumer<BridgeRemoveEvent> {
        @Override
        public void consume(ReactorEventBus eventBus, BridgeRemoveEvent event) {
            syncBridges(event.getBridgeNames());
        }
    }
}
