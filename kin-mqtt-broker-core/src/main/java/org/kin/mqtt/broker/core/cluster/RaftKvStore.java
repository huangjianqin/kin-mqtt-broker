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
import io.scalecube.reactor.RetryNonSerializedEmitFailureHandler;
import org.kin.framework.collection.Tuple;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.MqttBrokerConfig;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttBrokerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static io.scalecube.reactor.RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED;
import static org.kin.mqtt.broker.core.cluster.ClusterStoreKeys.BRIDGE_KEY_PREFIX;
import static org.kin.mqtt.broker.core.cluster.ClusterStoreKeys.SESSION_KEY_PREFIX;

/**
 * 基于jraft-rheakv实现的集群数据访问接口
 *
 * @author huangjianqin
 * @date 2023/5/18
 */
public class RaftKvStore implements ClusterStore {
    private static final Logger log = LoggerFactory.getLogger(RaftKvStore.class);
    /** 默认raft集群名称 */
    private static final String CLUSTER_NAME = "KinMqttBroker-kvStore";
    /** jraft-rheakv store切分的region数量 */
    private static final int REGION_NUM = 2;

    /** mqtt broker cluster */
    private final Cluster cluster;
    /** mqtt broker context */
    private final MqttBrokerContext brokerContext;
    /** jraft-rheakv store */
    private Mono<DefaultRheaKVStore> kvStore;

    public RaftKvStore(MqttBrokerContext brokerContext,
                       Cluster cluster) {
        this.brokerContext = brokerContext;
        this.cluster = cluster;
    }

    @Override
    public Mono<Void> init() {
        Sinks.One<DefaultRheaKVStore> onStart = Sinks.one();
        kvStore = onStart.asMono();
        return initStore(onStart)
                .doOnError(th -> onStart.emitError(th, RETRY_NON_SERIALIZED));
    }

    /**
     * 初始化raft kv store
     */
    private Mono<Void> initStore(Sinks.One<DefaultRheaKVStore> onStart) {
        MqttBrokerConfig brokerConfig = cluster.getBrokerContext().getBrokerConfig();

        DefaultRheaKVStore kvStore = new DefaultRheaKVStore();
        //先添加StateListener
        List<Mono<Void>> regionMonoList = new ArrayList<>(REGION_NUM);
        for (int i = 1; i <= REGION_NUM; i++) {
            int finalI = i;
            Sinks.One<Void> regionSink = Sinks.one();
            regionMonoList.add(regionSink.asMono());

            kvStore.addStateListener(i, new StateListener() {
                private volatile Sinks.One<Void> sink = regionSink;

                @Override
                public void onLeaderStart(long newTerm) {
                    log.info("raft kv store region-{} become leader", finalI);
                    onReady();

                    for (MqttBrokerNode node : cluster.getBrokerManager().getClusterBrokerNodes()) {
                        String storeAddress = node.getStoreAddress();
                        if (node.isCore()) {
                            addCore(storeAddress);
                        }
                        else{
                            addReplicator(storeAddress);
                        }
                    }
                }

                @Override
                public void onLeaderStop(long oldTerm) {
                    log.info("raft kv store region-{} become follower", finalI);
                }

                @Override
                public void onStartFollowing(PeerId newLeaderId, long newTerm) {
                    log.info("raft kv store region-{} start follow node '{}'", finalI, newLeaderId.getEndpoint());
                    onReady();
                }

                @Override
                public void onStopFollowing(PeerId oldLeaderId, long oldTerm) {
                    log.info("raft kv store region-{} stop follow '{}'", finalI, oldLeaderId.getEndpoint());
                }

                /**
                 * region raft node状态正常
                 */
                private void onReady() {
                    if (Objects.isNull(sink)) {
                        return;
                    }

                    sink.emitEmpty(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                    //help gc
                    sink = null;
                }
            });
        }

        if (cluster.isCore()) {
            log.info(getClass().getSimpleName() + " play core node");
            initCoreStore(kvStore, brokerConfig);
        } else {
            log.info(getClass().getSimpleName() + " play replicator node");
            initReplicatorStore(kvStore, brokerConfig);
        }

        return Mono.when(regionMonoList)
                .doOnSuccess(v -> {
                    log.info("RaftKvStore start successfully");
                    onStart.emitValue(kvStore, RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                });
    }

    /**
     * 初始化core节点raft kv store, 参与raft选举
     *
     * @param brokerConfig mqtt broker配置
     */
    private void initCoreStore(DefaultRheaKVStore kvStore, MqttBrokerConfig brokerConfig) {
        ClusterConfig clusterConfig = brokerConfig.getCluster();
        int storeProcessors = clusterConfig.getStoreProcessors();

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

        Endpoint endpoint = new Endpoint(clusterConfig.getHost(), clusterConfig.getStorePort());
        RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured()
                .withClusterName(CLUSTER_NAME)
                .withUseParallelCompress(true)
                .withInitialServerList(clusterConfig.getStoreSeeds())
                .withStoreEngineOptions(StoreEngineOptionsConfigured.newConfigured()
                        .withStorageType(StorageType.RocksDB)
                        .withRocksDBOptions(RocksDBOptionsConfigured.newConfigured()
                                .withDbPath(brokerConfig.getDataPath() + "/db")
                                .withSync(true)
                                .config())
                        .withRaftDataPath(brokerConfig.getDataPath() + "/raft")
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
                                .withStartKey(1L, BRIDGE_KEY_PREFIX)
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

        if (!kvStore.init(opts)) {
            throw new MqttBrokerException("fail to init raft kv core store on " + endpoint);
        }
    }

    /**
     * 初始化replicator节点raft kv store, 不参与raft选举, 仅仅从leader拉取数据
     *
     * @param brokerConfig mqtt broker配置
     */
    private void initReplicatorStore(DefaultRheaKVStore kvStore, MqttBrokerConfig brokerConfig) {
        ClusterConfig clusterConfig = brokerConfig.getCluster();
        int storeProcessors = clusterConfig.getStoreProcessors();

        //blot线程
//        int tpMinSize = Integer.parseInt(Configs.TP_MIN_SIZE_DEFAULT);
        System.setProperty(Configs.TP_MIN_SIZE, storeProcessors * 2 + "");
//        int tpMaxSize = Integer.parseInt(Configs.TP_MAX_SIZE_DEFAULT);
        System.setProperty(Configs.TP_MAX_SIZE, storeProcessors * 4 + "");

        //会被withRaftRpcCoreThreads覆盖
//        NodeOptions nodeOptions = new NodeOptions();
        //Utils.cpus() * 6, core是该值/3
//        nodeOptions.setRaftRpcThreadPoolSize(Utils.cpus() * 3);

        Endpoint endpoint = new Endpoint(clusterConfig.getHost(), clusterConfig.getStorePort());
        RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured()
                .withClusterName(CLUSTER_NAME)
                .withUseParallelCompress(true)
                .withInitialServerList(clusterConfig.getStoreSeeds() + "," + endpoint)
                .withStoreEngineOptions(StoreEngineOptionsConfigured.newConfigured()
                        .withStorageType(StorageType.RocksDB)
                        .withRocksDBOptions(RocksDBOptionsConfigured.newConfigured()
                                .withDbPath(brokerConfig.getDataPath() + "/db")
                                .withSync(true)
                                .config())
                        .withRaftDataPath(brokerConfig.getDataPath() + "/raft")
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
                                .withStartKey(1L, BRIDGE_KEY_PREFIX)
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
                                .withStartKey(1L, BRIDGE_KEY_PREFIX)
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

        if (!kvStore.init(opts)) {
            throw new MqttBrokerException("fail to init raft kv replicator store on " + endpoint);
        }
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

    /**
     * 底层异常封装
     */
    private Throwable onErrorMap(String oprDesc, Throwable source) {
        return new MqttBrokerException(getClass().getSimpleName() + " fail to " + oprDesc + " due to " + source.getMessage(), source);
    }

    @Override
    public <T> Mono<T> get(String key, Class<T> type) {
        checkInit();
        return kvStore.flatMap(s -> Mono.fromFuture(s.get(toBKey(key))))
                .onErrorMap(t -> onErrorMap(String.format("[GET] key '%s'", key), t))
                .map(bytes -> JSON.read(bytes, type));
    }

    @Override
    public Mono<byte[]> get(String key) {
        checkInit();
        //readOnlySafe=false, 将直接读取本地db数据, 但不保证数据一致性
        return kvStore.flatMap(s -> Mono.fromFuture(s.get(toBKey(key)))
                .onErrorMap(t -> onErrorMap(String.format("[GET] key '%s'", key), t)));
    }

    @Override
    public <T> Flux<Tuple<String, T>> multiGet(List<String> keys, Class<T> type) {
        checkInit();
        if (CollectionUtils.isEmpty(keys)) {
            return Flux.empty();
        }

        List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());

        return kvStore.flatMap(s -> Mono.fromFuture(s.multiGet(bKeys)))
                .onErrorMap(t -> onErrorMap(String.format("[MULTI-GET] keys '%s'", keys), t))
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
        if (CollectionUtils.isEmpty(keys)) {
            return Flux.empty();
        }

        List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());

        return kvStore.flatMap(s -> Mono.fromFuture(s.multiGet(bKeys)))
                .onErrorMap(t -> onErrorMap(String.format("[MULTI-GET] keys '%s'", keys), t))
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
        return kvStore.flatMap(s -> Mono.fromFuture(s.put(toBKey(key), JSON.writeBytes(obj)))
                        .onErrorMap(t -> onErrorMap(String.format("[PUT] key '%s'", key), t)))
                .then();
    }

    @Override
    public Mono<Void> put(Map<String, Object> kvs) {
        checkInit();
        if (CollectionUtils.isEmpty(kvs)) {
            return Mono.empty();
        }

        List<KVEntry> kvEntries = kvs.entrySet()
                .stream()
                .map(e -> new KVEntry(toBKey(e.getKey()), JSON.writeBytes(e.getValue())))
                .collect(Collectors.toList());
        return kvStore.flatMap(s -> Mono.fromFuture(s.put(kvEntries))
                        .onErrorMap(t -> onErrorMap(String.format("[PUT] key-values '%s'", kvs), t)))
                .then();
    }

    @Override
    public <T> Flux<Tuple<String, T>> scan(String startKey, String endKey, Class<T> type) {
        checkInit();
        return kvStore.flatMap(s -> Mono.fromFuture(s.scan(StringUtils.isNotBlank(startKey) ? toBKey(startKey) : null,
                        StringUtils.isNotBlank(endKey) ? toBKey(endKey) : null)))
                .onErrorMap(t -> onErrorMap(String.format("[SCAN] [start key, end key] '%s,%s'", startKey, endKey), t))
                .flatMapMany(list -> {
                    List<Tuple<String, T>> tuples = new ArrayList<>(list.size());
                    for (KVEntry entry : list) {
                        tuples.add(new Tuple<>(toSKey(entry.getKey()), JSON.read(entry.getValue(), type)));
                    }

                    return Flux.fromIterable(tuples);
                });
    }

    @Override
    public Flux<Tuple<String, byte[]>> scanRaw(String startKey, String endKey) {
        checkInit();
        return kvStore.flatMap(s -> Mono.fromFuture(s.scan(StringUtils.isNotBlank(startKey) ? toBKey(startKey) : null,
                        StringUtils.isNotBlank(endKey) ? toBKey(endKey) : null)))
                .onErrorMap(t -> onErrorMap(String.format("[SCAN] [start key, end key] '%s,%s'", startKey, endKey), t))
                .flatMapMany(list -> {
                    List<Tuple<String, byte[]>> tuples = new ArrayList<>(list.size());
                    for (KVEntry entry : list) {
                        tuples.add(new Tuple<>(toSKey(entry.getKey()), entry.getValue()));
                    }

                    return Flux.fromIterable(tuples);
                });
    }

    @Override
    public Mono<Void> delete(String key) {
        checkInit();
        return kvStore.flatMap(s -> Mono.fromFuture(s.delete(key))
                        .onErrorMap(t -> onErrorMap(String.format("[DELETE] key '%s'", key), t)))
                .then();
    }

    @Override
    public void addReplicator(String nodeAddress) {
        checkInit();
        kvStore.doOnNext(s -> {
                    for (RegionEngine regionEngine : s.getStoreEngine().getAllRegionEngines()) {
                        addReplicator(nodeAddress, regionEngine);
                    }
                })
                .subscribe();
    }

    /**
     * 添加replicator
     *
     * @param nodeAddress  replicator node address
     * @param regionEngine kv store region engine
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
        log.info("raft kv store region-{} ready to add replicator node '{}'", regionId, nodeAddress);

        PeerId newLearnerPeerId = PeerId.parsePeer(nodeAddress);
        boolean learnerExists = false;
        for (PeerId learnerPeerId : node.listLearners()) {
            if (!learnerPeerId.equals(newLearnerPeerId)) {
                continue;
            }

            learnerExists = true;
            break;
        }

        if (learnerExists) {
            log.info("raft kv store region-{} replicator node '{}' exists", regionId, nodeAddress);
        } else {
            node.addLearners(Collections.singletonList(newLearnerPeerId), status -> {
                if (!status.isOk()) {
                    // TODO: 2023/5/19 确认延迟时间
                    Mono.delay(Duration.ofMillis(1000))
                            .subscribeOn(brokerContext.getMqttBizScheduler())
                            .subscribe(t -> addReplicator(nodeAddress, regionEngine));
                    return;
                }
                log.info("raft kv store region-{} add replicator node '{}' result: {}", regionId, nodeAddress, status);
            });
        }
    }

    @Override
    public void removeReplicator(String nodeAddress) {
        checkInit();
        kvStore.doOnNext(s -> {
                    for (RegionEngine regionEngine : s.getStoreEngine().getAllRegionEngines()) {
                        removeReplicator(nodeAddress, regionEngine);
                    }
                })
                .subscribe();
    }

    /**
     * 移除replicator
     *
     * @param nodeAddress  replicator node address
     * @param regionEngine kv store region engine
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
        log.info("raft kv store region-{} ready to remove replicator node '{}'", regionId, nodeAddress);

        PeerId targetLearnerPeerId = PeerId.parsePeer(nodeAddress);
        boolean learnerExists = false;
        for (PeerId learnerPeerId : node.listLearners()) {
            if (!learnerPeerId.equals(targetLearnerPeerId)) {
                continue;
            }

            learnerExists = true;
            break;
        }

        if (learnerExists) {
            node.removeLearners(Collections.singletonList(targetLearnerPeerId), status -> {
                if (!status.isOk()) {
                    // TODO: 2023/5/19 确认延迟时间
                    Mono.delay(Duration.ofMillis(1000))
                            .subscribeOn(brokerContext.getMqttBizScheduler())
                            .subscribe(t -> removeReplicator(nodeAddress, regionEngine));
                    return;
                }
                log.info("raft kv store region-{} remove replicator node '{}' result: {}", regionId, nodeAddress, status);
            });
        } else {
            log.info("raft kv store region-{} replicator node '{}' not exists", regionId, nodeAddress);
        }
    }

    @Override
    public void addCore(String nodeAddress) {
        checkInit();
        kvStore.doOnNext(s -> {
                    for (RegionEngine regionEngine : s.getStoreEngine().getAllRegionEngines()) {
                        addCore(nodeAddress, regionEngine);
                    }
                })
                .subscribe();
    }

    /**
     * 添加core
     *
     * @param nodeAddress  core node address
     * @param regionEngine kv store region engine
     */
    private void addCore(String nodeAddress, RegionEngine regionEngine) {
        if (regionEngine == null) {
            return;
        }

        Node node = regionEngine.getNode();

        if (!node.isLeader()) {
            return;
        }

        long regionId = regionEngine.getRegion().getId();
        log.info("raft kv store region-{} ready to add core node '{}'", regionId, nodeAddress);

        PeerId newPeerId = PeerId.parsePeer(nodeAddress);
        boolean exists = false;
        for (PeerId peerId : node.listPeers()) {
            if (!peerId.equals(newPeerId)) {
                continue;
            }

            exists = true;
            break;
        }

        if (exists) {
            log.info("raft kv store region-{} core node '{}' exists", regionId, nodeAddress);
        } else {
            node.addPeer(newPeerId, status -> {
                if (!status.isOk()) {
                    // TODO: 2023/5/19 确认延迟时间
                    Mono.delay(Duration.ofMillis(1000))
                            .subscribeOn(brokerContext.getMqttBizScheduler())
                            .subscribe(t -> addCore(nodeAddress, regionEngine));
                    return;
                }
                log.info("raft kv store region-{} add core node '{}' result: {}", regionId, nodeAddress, status);
            });
        }
    }

    @Override
    public void removeCore(String nodeAddress) {
        checkInit();
        kvStore.doOnNext(s -> {
                    for (RegionEngine regionEngine : s.getStoreEngine().getAllRegionEngines()) {
                        removeCore(nodeAddress, regionEngine);
                    }
                })
                .subscribe();
    }

    /**
     * 移除core
     *
     * @param nodeAddress  core node address
     * @param regionEngine kv store region engine
     */
    private void removeCore(String nodeAddress, RegionEngine regionEngine) {
        if (regionEngine == null) {
            return;
        }

        Node node = regionEngine.getNode();

        if (!node.isLeader()) {
            return;
        }

        long regionId = regionEngine.getRegion().getId();
        log.info("raft kv store region-{} ready to remove core node '{}'", regionId, nodeAddress);

        PeerId targetPeerId = PeerId.parsePeer(nodeAddress);
        boolean exists = false;
        for (PeerId peerId : node.listPeers()) {
            if (!peerId.equals(targetPeerId)) {
                continue;
            }

            exists = true;
            break;
        }

        if (exists) {
            node.removePeer(targetPeerId, status -> {
                if (!status.isOk()) {
                    // TODO: 2023/5/19 确认延迟时间
                    Mono.delay(Duration.ofMillis(1000))
                            .subscribeOn(brokerContext.getMqttBizScheduler())
                            .subscribe(t -> removeCore(nodeAddress, regionEngine));
                    return;
                }
                log.info("raft kv store region-{} remove core node '{}' result: {}", regionId, nodeAddress, status);
            });
        } else {
            log.info("raft kv store region-{} core node '{}' not exists", regionId, nodeAddress);
        }
    }

    @Override
    public Mono<Void> shutdown() {
        checkInit();
        return kvStore.doOnNext(DefaultRheaKVStore::shutdown)
                .then();
    }
}
