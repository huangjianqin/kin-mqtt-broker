package org.kin.mqtt.broker.core.cluster;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.options.configured.RocksDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import io.scalecube.reactor.RetryNonSerializedEmitFailureHandler;
import org.apache.commons.io.FileUtils;
import org.kin.framework.collection.Tuple;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerException;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 参考{@link RocksRawKVStore}
 * @see com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore
 * @author huangjianqin
 * @date 2023/5/19
 */
public class StandaloneKvStore implements ClusterStore {
    private static final Logger log = LoggerFactory.getLogger(StandaloneKvStore.class);

    static {
        RocksDB.loadLibrary();
    }

    /** mqtt broker cluster */
    private final Cluster cluster;
    /** rocks store */
    private final RocksRawKVStore rocksStore = new RocksRawKVStore();

    public StandaloneKvStore(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void init() {
        String dbPath = cluster.getConfig().getDataPath() + "/db";

        try {
            FileUtils.forceMkdir(new File(dbPath));
        } catch (Throwable t) {
            log.error("fail to make dir for dbPath {}.", dbPath);
            ExceptionUtils.throwExt(t);
        }

        RocksDBOptions options = RocksDBOptionsConfigured.newConfigured()
                .withDbPath(dbPath)
                .withSync(true)
                .config();

        rocksStore.init(options);
    }

    /**
     * 检查store是否已经初始化
     */
    private void checkInit() {
        if (rocksStore == null) {
            throw new MqttBrokerException("standalone kv store is not init");
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
        return get(key)
                .map(bytes -> JSON.read(bytes, type));
    }

    @Override
    public Mono<byte[]> get(String key) {
        checkInit();
        Sinks.One<byte[]> sink = Sinks.one();
        rocksStore.get(toBKey(key), true, new BaseKVStoreClosure() {
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    sink.emitValue((byte[]) getData(), RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                }
                else{
                    sink.emitEmpty(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                }
            }
        });
        return sink.asMono();
    }

    @Override
    public <T> Flux<Tuple<String, T>> multiGet(List<String> keys, Class<T> type) {
        checkInit();

        if(CollectionUtils.isEmpty(keys)){
            return Flux.empty();
        }

        Sinks.Many<Tuple<String, T>> sink = Sinks.many().multicast().onBackpressureBuffer();
        List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());
        rocksStore.multiGet(bKeys, new BaseKVStoreClosure() {
            @SuppressWarnings("unchecked")
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    Map<ByteArray, byte[]> map = (Map<ByteArray, byte[]>) getData();
                    for (Map.Entry<ByteArray, byte[]> entry : map.entrySet()) {
                        ByteArray bKey = entry.getKey();
                        byte[] bValue = entry.getValue();

                        sink.emitNext(new Tuple<>(toSKey(bKey), JSON.read(bValue, type)),
                                RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                    }
                }
                else{
                    sink.emitComplete(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                }
            }
        });

        return sink.asFlux();
    }

    @Override
    public Flux<Tuple<String, byte[]>> multiGetRaw(List<String> keys) {
        checkInit();

        if(CollectionUtils.isEmpty(keys)){
            return Flux.empty();
        }

        Sinks.Many<Tuple<String, byte[]>> sink = Sinks.many().multicast().onBackpressureBuffer();
        List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());
        rocksStore.multiGet(bKeys, new BaseKVStoreClosure() {
            @SuppressWarnings("unchecked")
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    Map<ByteArray, byte[]> map = (Map<ByteArray, byte[]>) getData();
                    for (Map.Entry<ByteArray, byte[]> entry : map.entrySet()) {
                        ByteArray bKey = entry.getKey();
                        byte[] bValue = entry.getValue();

                        sink.emitNext(new Tuple<>(toSKey(bKey), bValue), RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                    }
                }
                else{
                    sink.emitComplete(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                }
            }
        });

        return sink.asFlux();
    }

    @Override
    public Mono<Void> put(String key, Object obj) {
        checkInit();

        Sinks.One<Void> sink = Sinks.one();
        rocksStore.put(toBKey(key), JSON.writeBytes(obj), new BaseKVStoreClosure() {
            @Override
            public void run(Status status) {
                sink.emitEmpty(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
            }
        });
        return sink.asMono();
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

        Sinks.One<Void> sink = Sinks.one();
        rocksStore.put(kvEntries , new BaseKVStoreClosure() {
            @Override
            public void run(Status status) {
                sink.emitEmpty(RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
            }
        });
        return sink.asMono();
    }

    @Override
    public void addReplicator(String nodeAddress) {
        //do nothing
    }

    @Override
    public void removeReplicator(String nodeAddress) {
        //do nothing
    }

    @Override
    public void shutdown() {
        rocksStore.shutdown();
    }
}
