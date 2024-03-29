package org.kin.mqtt.broker.core.cluster;

import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.options.configured.RocksDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.rocks.support.RocksStatisticsCollector;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.DebugStatistics;
import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.alipay.sofa.jraft.util.concurrent.AdjustableSemaphore;
import org.apache.commons.io.FileUtils;
import org.kin.framework.collection.Tuple;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.MqttBrokerException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static io.scalecube.reactor.RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED;

/**
 * 参考{@link RocksRawKVStore}
 * 因为{@link RocksRawKVStore}实现里面{@link RocksRawKVStore#init(RocksDBOptions)}会先删除db, 然后新建db, 并基于snapshot and log playback来恢复DB数据
 * 删除db是强制操作, 无法通过配置或继承来屏蔽, 因此只能重新实现, 代码几乎一样的, 只是做了简单的reactor化
 *
 * @author huangjianqin
 * @date 2023/5/19
 */
public class StandaloneKvStore implements ClusterStore {
    private static final Logger log = LoggerFactory.getLogger(StandaloneKvStore.class);

    static {
        RocksDB.loadLibrary();
    }

    private final AdjustableSemaphore shutdownLock = new AdjustableSemaphore();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final AtomicLong databaseVersion = new AtomicLong(0);
    private final List<ColumnFamilyOptions> cfOptionsList = Lists.newArrayList();
    private final List<ColumnFamilyDescriptor> cfDescriptors = Lists.newArrayList();
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle sequenceHandle;
    private ColumnFamilyHandle lockingHandle;
    private ColumnFamilyHandle fencingHandle;
    private Mono<RocksDB> db;
    private DBOptions dbOptions;
    private WriteOptions writeOptions;
    private DebugStatistics statistics;
    private RocksStatisticsCollector statisticsCollector;

    /** mqtt broker cluster */
    private final Cluster cluster;

    public StandaloneKvStore(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Mono<Void> init() {
        Sinks.One<RocksDB> onStart = Sinks.one();
        db = onStart.asMono();
        return  init(onStart)
                .doOnError(th -> onStart.emitError(th, RETRY_NON_SERIALIZED));
    }

    /**
     * 初始化rocks db
     */
    private Mono<Void> init(Sinks.One<RocksDB> onStart) {
        String dbPath = cluster.getBrokerContext().getBrokerConfig().getDataPath() + "/db";

        try {
            FileUtils.forceMkdir(new File(dbPath));
        } catch (Throwable t) {
            log.error("fail to make dir for dbPath {}.", dbPath);
            ExceptionUtils.throwExt(t);
        }

        Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            RocksDBOptions opts = RocksDBOptionsConfigured.newConfigured()
                    .withDbPath(dbPath)
                    .withSync(true)
                    .config();
            this.dbOptions = createDBOptions();
            if (opts.isOpenStatisticsCollector()) {
                this.statistics = new DebugStatistics();
                this.dbOptions.setStatistics(this.statistics);
                long intervalSeconds = opts.getStatisticsCallbackIntervalSeconds();
                if (intervalSeconds > 0) {
                    this.statisticsCollector = new RocksStatisticsCollector(TimeUnit.SECONDS.toMillis(intervalSeconds));
                    this.statisticsCollector.start();
                }
            }
            ColumnFamilyOptions cfOptions = createColumnFamilyOptions();
            this.cfOptionsList.add(cfOptions);
            // default column family
            this.cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            // sequence column family
            this.cfDescriptors.add(new ColumnFamilyDescriptor(BytesUtil.writeUtf8("RHEA_SEQUENCE"), cfOptions));
            // locking column family
            this.cfDescriptors.add(new ColumnFamilyDescriptor(BytesUtil.writeUtf8("RHEA_LOCKING"), cfOptions));
            // fencing column family
            this.cfDescriptors.add(new ColumnFamilyDescriptor(BytesUtil.writeUtf8("RHEA_FENCING"), cfOptions));
            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(opts.isSync());
            // If `sync` is true, `disableWAL` must be set false.
            this.writeOptions.setDisableWAL(!opts.isSync() && opts.isDisableWAL());


            List<ColumnFamilyHandle> cfHandles = Lists.newArrayList();
            this.databaseVersion.incrementAndGet();
            onStart.emitValue(RocksDB.open(this.dbOptions, opts.getDbPath(), this.cfDescriptors, cfHandles), RETRY_NON_SERIALIZED);
            this.defaultHandle = cfHandles.get(0);
            this.sequenceHandle = cfHandles.get(1);
            this.lockingHandle = cfHandles.get(2);
            this.fencingHandle = cfHandles.get(3);


            this.shutdownLock.setMaxPermits(1);
            log.info("StandaloneKvStore start successfully, options: {}.", opts);
        } catch (Exception e) {
            log.error("fail to open rocksDB at path {}, {}.", dbPath, StackTraceUtil.stackTrace(e));
        } finally {
            writeLock.unlock();
        }

        return Mono.empty();
    }

    // Creates the rocksDB options, the user must take care
    // to close it after closing db.
    private static DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(RocksRawKVStore.class) //
                .setEnv(Env.getDefault());
    }

    // Creates the column family options to control the behavior
    // of a database.
    private static ColumnFamilyOptions createColumnFamilyOptions() {
        BlockBasedTableConfig tConfig = StorageOptionsFactory.getRocksDBTableFormatConfig(RocksRawKVStore.class);
        return StorageOptionsFactory.getRocksDBColumnFamilyOptions(RocksRawKVStore.class) //
                .setTableFormatConfig(tConfig) //
                .setMergeOperator(new StringAppendOperator());
    }

    /**
     * 检查store是否已经初始化
     */
    private void checkInit() {
        if (db == null) {
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

    /**
     * 底层异常封装
     */
    private void onError(String oprDesc, Throwable source) {
        throw new MqttBrokerException(getClass().getSimpleName() + " fail to " + oprDesc + " due to " + source.getMessage(), source);
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
        return db.mapNotNull(idb -> {
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                return idb.get(toBKey(key));
            } catch (Exception e) {
                onError(String.format("[GET] key '%s'", key), e);
                return null;
            } finally {
                readLock.unlock();
            }
        });
    }

    @Override
    public <T> Flux<Tuple<String, T>> multiGet(List<String> keys, Class<T> type) {
        checkInit();
        if (CollectionUtils.isEmpty(keys)) {
            return Flux.empty();
        }

        return db.flatMapMany(idb -> Flux.create(sink -> {
            List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());

            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                List<byte[]> rawList = idb.multiGetAsList(bKeys);

                int index = 0;
                for (byte[] bValue : rawList) {
                    String key = keys.get(index);
                    index++;

                    sink.next(new Tuple<>(key, Objects.nonNull(bValue)? JSON.read(bValue, type): null));
                }
            } catch (Exception e) {
                onError(String.format("[MULTI-GET] keys '%s'", keys), e);
            } finally {
                sink.complete();
                readLock.unlock();
            }
        }));
    }

    @Override
    public Flux<Tuple<String, byte[]>> multiGetRaw(List<String> keys) {
        checkInit();
        if (CollectionUtils.isEmpty(keys)) {
            return Flux.empty();
        }

        return db.flatMapMany(idb -> Flux.create(sink -> {
            List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());

            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                List<byte[]> rawList = idb.multiGetAsList(bKeys);

                int index = 0;
                for (byte[] value : rawList) {
                    String key = keys.get(index);
                    index++;

                    sink.next(new Tuple<>(key, value));
                }
            } catch (Exception e) {
                onError(String.format("[MULTI-GET] keys '%s'", keys), e);
            } finally {
                sink.complete();
                readLock.unlock();
            }
        }));
    }

    @Override
    public Mono<Void> put(String key, Object obj) {
        checkInit();
        return db.doOnNext(idb -> {
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                idb.put(this.writeOptions, toBKey(key), JSON.writeBytes(obj));
            } catch (Exception e) {
                onError(String.format("[PUT] key '%s'", key), e);
            } finally {
                readLock.unlock();
            }
        }).then();
    }

    @Override
    public Mono<Void> put(Map<String, Object> kvs) {
        checkInit();
        if (CollectionUtils.isEmpty(kvs)) {
            return Mono.empty();
        }

        return db.doOnNext(idb ->  {
            List<KVEntry> kvEntries = kvs.entrySet()
                    .stream()
                    .map(e -> new KVEntry(toBKey(e.getKey()), JSON.writeBytes(e.getValue())))
                    .collect(Collectors.toList());

            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try (WriteBatch batch = new WriteBatch()) {
                for (KVEntry entry : kvEntries) {
                    batch.put(entry.getKey(), entry.getValue());
                }
                idb.write(this.writeOptions, batch);
            } catch (Exception e) {
                onError(String.format("[PUT] key-values '%s'", kvs), e);
            } finally {
                readLock.unlock();
            }
        }).then();
    }

    @Override
    public <T> Flux<Tuple<String, T>> scan(String startKey, String endKey, Class<T> type) {
        checkInit();
        return db.flatMapMany(idb -> Flux.create(sink -> {
            int maxCount = Integer.MAX_VALUE;
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try (RocksIterator it = idb.newIterator()) {
                if (startKey == null) {
                    it.seekToFirst();
                } else {
                    it.seek(toBKey(startKey));
                }
                int count = 0;
                byte[] endBKey = endKey != null ? toBKey(endKey) : null;
                while (it.isValid() && count++ < maxCount) {
                    byte[] key = it.key();
                    if (endBKey != null && BytesUtil.compare(key, endBKey) >= 0) {
                        break;
                    }
                    byte[] bValue = it.value();
                    sink.next(new Tuple<>(toSKey(key), Objects.nonNull(bValue)? JSON.read(bValue, type): null));
                    it.next();
                }
            } catch (Exception e) {
                onError(String.format("[SCAN] [start key, end key] '%s,%s'", startKey, endKey), e);
            } finally {
                sink.complete();
                readLock.unlock();
            }
        }));
    }

    @Override
    public Flux<Tuple<String, byte[]>> scanRaw(String startKey, String endKey) {
        checkInit();
        return db.flatMapMany(idb -> Flux.create(sink -> {
            int maxCount = Integer.MAX_VALUE;
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try (RocksIterator it = idb.newIterator()) {
                if (startKey == null) {
                    it.seekToFirst();
                } else {
                    it.seek(toBKey(startKey));
                }
                int count = 0;
                byte[] endBKey = endKey != null ? toBKey(endKey) : null;
                while (it.isValid() && count++ < maxCount) {
                    byte[] key = it.key();
                    if (endBKey != null && BytesUtil.compare(key, endBKey) >= 0) {
                        break;
                    }
                    sink.next(new Tuple<>(toSKey(key), it.value()));
                    it.next();
                }
            } catch (Exception e) {
                onError(String.format("[SCAN] [start key, end key] '%s,%s'", startKey, endKey), e);
            } finally {
                sink.complete();
                readLock.unlock();
            }
        }));
    }

    @Override
    public Mono<Void> delete(String key) {
        checkInit();
        return db.doOnNext(idb -> {
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                idb.delete(this.writeOptions, toBKey(key));
            } catch (Exception e) {
                onError(String.format("[DELETE] key '%s'", key), e);
            } finally {
                readLock.unlock();
            }
        }).then();

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
    public void addCore(String nodeAddress) {
        //do nothing
    }

    @Override
    public void removeCore(String nodeAddress) {
        //do nothing
    }

    @Override
    public Mono<Void> shutdown() {
        checkInit();
        return db.doOnNext(this::shutdown0)
                .then();
    }

    private void shutdown0(RocksDB db){
        Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            this.shutdownLock.setMaxPermits(0);
            db.close();
            if (this.defaultHandle != null) {
                this.defaultHandle.close();
                this.defaultHandle = null;
            }
            if (this.sequenceHandle != null) {
                this.sequenceHandle.close();
                this.sequenceHandle = null;
            }
            if (this.lockingHandle != null) {
                this.lockingHandle.close();
                this.lockingHandle = null;
            }
            if (this.fencingHandle != null) {
                this.fencingHandle.close();
                this.fencingHandle = null;
            }
            for (ColumnFamilyOptions cfOptions : this.cfOptionsList) {
                cfOptions.close();
            }
            this.cfOptionsList.clear();
            this.cfDescriptors.clear();
            if (this.dbOptions != null) {
                this.dbOptions.close();
                this.dbOptions = null;
            }
            if (this.statisticsCollector != null) {
                try {
                    this.statisticsCollector.shutdown(3000);
                } catch (Throwable ignored) {
                    // ignored
                }
            }
            if (this.statistics != null) {
                this.statistics.close();
                this.statistics = null;
            }
            if (this.writeOptions != null) {
                this.writeOptions.close();
                this.writeOptions = null;
            }
        } finally {
            writeLock.unlock();
            log.info("[StandaloneKvStore] shutdown successfully.");
        }
    }
}
