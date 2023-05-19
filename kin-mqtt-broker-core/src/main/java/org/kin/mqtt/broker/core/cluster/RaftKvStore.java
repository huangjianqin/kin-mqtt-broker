package org.kin.mqtt.broker.core.cluster;

import com.alipay.remoting.config.Configs;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.RegionEngine;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.configured.*;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.util.Endpoint;
import org.kin.framework.collection.Tuple;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttBrokerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.kin.mqtt.broker.core.cluster.ClusterStoreKeys.RULE_KEY_PREFIX;
import static org.kin.mqtt.broker.core.cluster.ClusterStoreKeys.SESSION_KEY_PREFIX;

/**
 * 基于jraft-rheakv实现的集群数据访问接口
 *
 * @author huangjianqin
 * @date 2023/5/18
 */
public class RaftKvStore implements ClusterStore{
    private static final Logger log = LoggerFactory.getLogger(RaftKvStore.class);
    /** 默认raft集群名称 */
    private static final String CLUSTER_NAME = "KinMqttBroker";
    /** jraft-rheakv store切分的region数量 */
    private static final int REGION_NUM = 2;

    /** mqtt broker cluster */
    private final Cluster cluster;
    /** mqtt broker context */
    private final MqttBrokerContext brokerContext;
    /** jraft-rheakv store */
    private DefaultRheaKVStore kvStore;

    public RaftKvStore(MqttBrokerContext brokerContext,
                       Cluster cluster) {
        this.brokerContext = brokerContext;
        this.cluster = cluster;
    }

    @Override
    public void init() {
        ClusterConfig config = cluster.getConfig();

        if (cluster.isCore()) {
            kvStore = initCoreStore(config);
        } else {
            kvStore = initReplicatorStore(config);
        }

        for (int i = 0; i < REGION_NUM; i++) {
            int finalI = i;
            kvStore.addStateListener(i, new StateListener() {
                @Override
                public void onLeaderStart(long newTerm) {
                    log.info("raft kv store region-{} become leader", finalI);

                    for (MqttBrokerNode node : cluster.getBrokerManager().getClusterBrokerNodes()) {
                        addReplicator(node.getHost() + ":" + node.getPort());
                    }
                }

                @Override
                public void onLeaderStop(long oldTerm) {
                    log.info("raft kv store region-{} become follower", finalI);
                }

                @Override
                public void onStartFollowing(PeerId newLeaderId, long newTerm) {
                    log.info("raft kv store region-{} start follow node '{}'", finalI, newLeaderId.getEndpoint());
                }

                @Override
                public void onStopFollowing(PeerId oldLeaderId, long oldTerm) {
                    log.info("raft kv store region-{} stop follow '{}'", finalI, oldLeaderId.getEndpoint());
                }
            });
        }
    }

    /**
     * 初始化core节点, 参与raft选举
     *
     * @param config mqtt broker集群配置
     */
    private DefaultRheaKVStore initCoreStore(ClusterConfig config) {
        int storeProcessors = config.getStoreProcessors();

        //线程数设计得考虑core+replicator混用
        //blot线程
//        int tpMinSize = Integer.parseInt(Configs.TP_MIN_SIZE_DEFAULT);
        System.setProperty(Configs.TP_MIN_SIZE, storeProcessors * 3 + "");
//        int tpMaxSize = Integer.parseInt(Configs.TP_MAX_SIZE_DEFAULT);
        System.setProperty(Configs.TP_MAX_SIZE, storeProcessors * 5 + "");

        //会被withRaftRpcCoreThreads覆盖
//        NodeOptions nodeOptions = new NodeOptions();
        //Utils.cpus() * 6, core是该值/3
//        nodeOptions.setRaftRpcThreadPoolSize(Utils.cpus() * 3);

        Endpoint endpoint = new Endpoint(config.getHost(), config.getStorePort());
        RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured()
                .withClusterName(CLUSTER_NAME)
                .withUseParallelCompress(true)
                .withInitialServerList(config.getSeeds())
                .withStoreEngineOptions(StoreEngineOptionsConfigured.newConfigured()
                        .withStorageType(StorageType.RocksDB)
                        .withRocksDBOptions(RocksDBOptionsConfigured.newConfigured()
                                .withDbPath(config.getDataPath() + "/db")
                                .withSync(true)
                                .config())
                        .withRaftDataPath(config.getDataPath() + "/raft")
                        .withServerAddress(endpoint)
                        //控制read index失败后重新请求leader线程数, Math.max(Utils.cpus() << 2, 16), max=core*4倍
                        .withReadIndexCoreThreads(storeProcessors)
                        //控制raft cli相关rpc command处理线程数, Utils.cpus() << 2, max=core*4倍
                        //用到cli api较少
                        .withCliRpcCoreThreads(storeProcessors / 2 + 1)
                        //控制raft相关rpc command处理线程数, Math.max(Utils.cpus() << 3, 32), max=core*2倍
                        .withRaftRpcCoreThreads(storeProcessors * 4)
                        //控制kv相关rpc command处理线程数, Math.max(Utils.cpus() << 3, 32), max=core*4倍
                        .withKvRpcCoreThreads(storeProcessors * 4)
                        .withRegionEngineOptionsList(MultiRegionEngineOptionsConfigured.newConfigured()
                                //region1: rule
                                .withStartKey(1L, RULE_KEY_PREFIX)
                                //region2: session + broker topic subscription
                                .withStartKey(2L, SESSION_KEY_PREFIX)
                                .config())
//                        .withCommonNodeOptions(nodeOptions)
                        //开启后, leadership会切换得频繁些
//                        .withUseSharedRpcExecutor(true)
                        .config())
                .withPlacementDriverOptions(PlacementDriverOptionsConfigured.newConfigured()
                        .withFake(true)
                        .config())
                .withOnlyLeaderRead(false)
                //Utils.cpus()
                .withCompressThreads(Math.min(3, storeProcessors))
                //Utils.cpus() + 1
                .withDeCompressThreads(Math.min(3, storeProcessors) + 1)
                .config();
        DefaultRheaKVStore rheaKVStore = new DefaultRheaKVStore();
        if (!rheaKVStore.init(opts)) {
            throw new MqttBrokerException("fail to init raft kv core store on " + endpoint);
        }

        return rheaKVStore;
    }

    /**
     * 初始化replicator节点, 不参与raft选举, 仅仅从leader拉取数据
     *
     * @param config mqtt broker集群配置
     */
    private DefaultRheaKVStore initReplicatorStore(ClusterConfig config) {
        int storeProcessors = config.getStoreProcessors();

        //blot线程
//        int tpMinSize = Integer.parseInt(Configs.TP_MIN_SIZE_DEFAULT);
        System.setProperty(Configs.TP_MIN_SIZE, storeProcessors * 2 + "");
//        int tpMaxSize = Integer.parseInt(Configs.TP_MAX_SIZE_DEFAULT);
        System.setProperty(Configs.TP_MAX_SIZE, storeProcessors * 4 + "");

        //会被withRaftRpcCoreThreads覆盖
//        NodeOptions nodeOptions = new NodeOptions();
        //Utils.cpus() * 6, core是该值/3
//        nodeOptions.setRaftRpcThreadPoolSize(Utils.cpus() * 3);

        Endpoint endpoint = new Endpoint(config.getHost(), config.getStorePort());
        RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured()
                .withClusterName(CLUSTER_NAME)
                .withUseParallelCompress(true)
                .withInitialServerList(config.getSeeds() + "," + endpoint)
                .withStoreEngineOptions(StoreEngineOptionsConfigured.newConfigured()
                        .withStorageType(StorageType.RocksDB)
                        .withRocksDBOptions(RocksDBOptionsConfigured.newConfigured()
                                .withDbPath(config.getDataPath() + "/db")
                                .withSync(true)
                                .config())
                        .withRaftDataPath(config.getDataPath() + "/raft")
                        .withServerAddress(endpoint)
                        //控制read index失败后重新请求leader线程数, Math.max(Utils.cpus() << 2, 16), max=core*4倍
                        .withReadIndexCoreThreads(storeProcessors)
                        //控制raft cli相关rpc command处理线程数, Utils.cpus() << 2, max=core*4倍
                        //用到cli api较少
                        .withCliRpcCoreThreads(storeProcessors / 2 + 1)
                        //控制raft相关rpc command处理线程数, Math.max(Utils.cpus() << 3, 32), max=core*2倍
                        .withRaftRpcCoreThreads(storeProcessors * 2)
                        //控制kv相关rpc command处理线程数, Math.max(Utils.cpus() << 3, 32), max=core*4倍
                        .withKvRpcCoreThreads(storeProcessors * 2)
                        .withRegionEngineOptionsList(MultiRegionEngineOptionsConfigured.newConfigured()
                                //region1: rule
                                .withStartKey(1L, RULE_KEY_PREFIX)
                                //region2: session + broker topic subscription
                                .withStartKey(2L, SESSION_KEY_PREFIX)
                                .config())
//                        .withCommonNodeOptions(nodeOptions)
                        //开启后, leadership会切换得频繁些
//                        .withUseSharedRpcExecutor(true)
                        .config())
                .withPlacementDriverOptions(PlacementDriverOptionsConfigured.newConfigured()
                        .withFake(true)
                        .withRegionRouteTableOptionsList(MultiRegionRouteTableOptionsConfigured
                                .newConfigured()
                                //region1: rule
                                .withStartKey(1L, RULE_KEY_PREFIX)
                                //region2: session + broker topic subscription
                                .withStartKey(2L, SESSION_KEY_PREFIX)
                                .config())
                        .config())
                .withOnlyLeaderRead(false)
                //Utils.cpus()
                .withCompressThreads(Math.min(3, storeProcessors))
                //Utils.cpus() + 1
                .withDeCompressThreads(Math.min(3, storeProcessors) + 1)
                .config();

        DefaultRheaKVStore rheaKVStore = new DefaultRheaKVStore();
        if (!rheaKVStore.init(opts)) {
            throw new MqttBrokerException("fail to init raft kv replicator store on " + endpoint);
        }

        return rheaKVStore;
    }

    /**
     * 检查store是否已经初始化
     */
    private void checkInit() {
        if (kvStore == null) {
            throw new MqttBrokerException("cluster store is not init");
        }
    }

    /**
     * 将string类型key转换成byte数组
     *
     * @return byte数组
     */
    private byte[] toBKey(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 将byte数组类型key转换成string
     *
     * @return string
     */
    private String toSKey(byte[] key) {
        return new String(key, StandardCharsets.UTF_8);
    }

    /**
     * 将byte数组类型key转换成string
     *
     * @return string
     */
    private String toSKey(ByteArray byteArray) {
        return new String(byteArray.getBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public <T> Mono<T> get(String key, Class<T> type) {
        checkInit();
        return Mono.fromFuture(kvStore.get(toBKey(key)))
                .map(bytes -> JSON.read(bytes, type));
    }

    @Override
    public Mono<byte[]> get(String key) {
        checkInit();
        return Mono.fromFuture(kvStore.get(toBKey(key)));
    }

    @Override
    public <T> Flux<Tuple<String, T>> multiGet(List<String> keys, Class<T> type) {
        checkInit();

        if(CollectionUtils.isEmpty(keys)){
            return Flux.empty();
        }

        List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());

        return Mono.fromFuture(kvStore.multiGet(bKeys))
                .flatMapMany(map -> {
                    List<Tuple<String, T>> tuples = new ArrayList<>(map.size());
                    for (Map.Entry<ByteArray, byte[]> entry : map.entrySet()) {
                        ByteArray bKey = entry.getKey();
                        byte[] bValue = entry.getValue();

                        tuples.add(new Tuple<>(toSKey(bKey), JSON.read(bValue, type)));
                    }

                    return Flux.fromIterable(tuples);
                });
    }

    @Override
    public Flux<Tuple<String, byte[]>> multiGetRaw(List<String> keys) {
        checkInit();

        if(CollectionUtils.isEmpty(keys)){
            return Flux.empty();
        }

        List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());

        return Mono.fromFuture(kvStore.multiGet(bKeys))
                .flatMapMany(map -> {
                    List<Tuple<String, byte[]>> tuples = new ArrayList<>(map.size());
                    for (Map.Entry<ByteArray, byte[]> entry : map.entrySet()) {
                        ByteArray bKey = entry.getKey();
                        byte[] bValue = entry.getValue();

                        tuples.add(new Tuple<>(toSKey(bKey), bValue));
                    }

                    return Flux.fromIterable(tuples);
                });
    }

    @Override
    public Mono<Void> put(String key, Object obj) {
        checkInit();
        return Mono.fromFuture(kvStore.put(toBKey(key), JSON.writeBytes(obj)))
                .then();
    }

    @Override
    public Mono<Void> put(Map<String, Object> kvs) {
        checkInit();

        if(CollectionUtils.isEmpty(kvs)){
            return Mono.empty();
        }

        List<KVEntry> kvEntries = kvs.entrySet()
                .stream()
                .map(e -> new KVEntry(toBKey(e.getKey()), JSON.writeBytes(e.getValue())))
                .collect(Collectors.toList());
        return Mono.fromFuture(kvStore.put(kvEntries))
                .then();
    }

    @Override
    public void addReplicator(String nodeAddress) {
        for (RegionEngine regionEngine : kvStore.getStoreEngine().getAllRegionEngines()) {
            addReplicator(nodeAddress, regionEngine);
        }
    }

    /**
     * 添加replicator
     * @param nodeAddress   replicator node address
     * @param regionId  kv store region id
     */
    private void addReplicator(String nodeAddress, long regionId) {
        addReplicator(nodeAddress, kvStore.getStoreEngine().getRegionEngine(regionId));
    }

    /**
     * 添加replicator
     * @param nodeAddress   replicator node address
     * @param regionEngine  kv store region engine
     */
    private void addReplicator(String nodeAddress, RegionEngine regionEngine) {
        if (regionEngine == null) {
            return;
        }

        Node node = regionEngine.getNode();

        if (!node.isLeader()) {
            return;
        }

        long regionId = regionEngine.getRegion().getId();
        log.info("raft kv store region-{} ready to add learner '{}'", regionId, nodeAddress);

        PeerId newLearnerPeerId = PeerId.parsePeer(nodeAddress);
        boolean learnerExists = false;
        for (PeerId learnerPeerId : node.listLearners()) {
            if (learnerPeerId.equals(newLearnerPeerId)) {
                learnerExists = true;
            }
        }

        if (learnerExists) {
            log.info("raft kv store region-{} learner '{}' exists", regionId, nodeAddress);
        } else {
            node.addLearners(Collections.singletonList(newLearnerPeerId), status -> {
                if(!status.isOk()){
                    // TODO: 2023/5/19 确认延迟时间
                    Mono.delay(Duration.ofMillis(1000))
                            .subscribeOn(brokerContext.getMqttBizScheduler())
                            .subscribe(t-> addReplicator(nodeAddress, regionId));
                    return;
                }
                log.info("raft kv store region-{} add learner '{}' result: {}", regionId, nodeAddress, status);
            });
        }
    }

    @Override
    public void removeReplicator(String nodeAddress) {
        for (RegionEngine regionEngine : kvStore.getStoreEngine().getAllRegionEngines()) {
            removeReplicator(nodeAddress, regionEngine);
        }
    }

    /**
     * 移除replicator
     * @param nodeAddress   replicator node address
     * @param regionId  kv store region id
     */
    private void removeReplicator(String nodeAddress, long regionId) {
        removeReplicator(nodeAddress, kvStore.getStoreEngine().getRegionEngine(regionId));
    }

    /**
     * 移除replicator
     * @param nodeAddress   replicator node address
     * @param regionEngine  kv store region engine
     */
    private void removeReplicator(String nodeAddress, RegionEngine regionEngine) {
        if (regionEngine == null) {
            return;
        }

        Node node = regionEngine.getNode();

        if (!node.isLeader()) {
            return;
        }

        long regionId = regionEngine.getRegion().getId();
        log.info("raft kv store region-{} ready to remove learner '{}'", regionId, nodeAddress);

        PeerId targetLearnerPeerId = PeerId.parsePeer(nodeAddress);
        boolean learnerExists = false;
        for (PeerId learnerPeerId : node.listLearners()) {
            if (learnerPeerId.equals(targetLearnerPeerId)) {
                learnerExists = true;
            }
        }

        if (learnerExists) {
            node.removeLearners(Collections.singletonList(targetLearnerPeerId), status -> {
                if(!status.isOk()){
                    // TODO: 2023/5/19 确认延迟时间
                    Mono.delay(Duration.ofMillis(1000))
                            .subscribeOn(brokerContext.getMqttBizScheduler())
                            .subscribe(t-> removeReplicator(nodeAddress, regionId));
                    return;
                }
                log.info("raft kv store region-{} remove learner '{}' result: {}", regionId, nodeAddress, status);
            });
        } else {
            log.info("raft kv store region-{} learner '{}' not exists", regionId, nodeAddress);
        }
    }

    @Override
    public void shutdown() {
        kvStore.shutdown();
    }
}
