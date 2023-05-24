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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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
    private RocksDB db;
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
            if (this.db != null) {
                log.info("{} already started.", getClass().getSimpleName());
                return Mono.empty();
            }
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
            openRocksDB(opts);
            this.shutdownLock.setMaxPermits(1);
            log.info("StandaloneKvStore start successfully, options: {}.", opts);
        } catch (Exception e) {
            log.error("fail to open rocksDB at path {}, {}.", dbPath, StackTraceUtil.stackTrace(e));
        } finally {
            writeLock.unlock();
        }

        return Mono.empty();
    }

    private void openRocksDB(RocksDBOptions opts) throws RocksDBException {
        List<ColumnFamilyHandle> cfHandles = Lists.newArrayList();
        this.databaseVersion.incrementAndGet();
        this.db = RocksDB.open(this.dbOptions, opts.getDbPath(), this.cfDescriptors, cfHandles);
        this.defaultHandle = cfHandles.get(0);
        this.sequenceHandle = cfHandles.get(1);
        this.lockingHandle = cfHandles.get(2);
        this.fencingHandle = cfHandles.get(3);
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

    @Override
    public <T> Mono<T> get(String key, Class<T> type) {
        checkInit();
        return get(key)
                .map(bytes -> JSON.read(bytes, type));
    }

    @Override
    public Mono<byte[]> get(String key) {
        checkInit();
        return Mono.fromCallable(() -> {
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                return this.db.get(toBKey(key));
            } catch (Exception e) {
                log.error("fail to [GET], key: [{}], {}.", key, StackTraceUtil.stackTrace(e));
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

        return Flux.create(sink -> {
            List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());

            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                List<byte[]> rawList = this.db.multiGetAsList(bKeys);

                int index = 0;
                for (byte[] value : rawList) {
                    String key = keys.get(index);
                    index++;

                    sink.next(new Tuple<>(key, JSON.read(value, type)));
                }

                sink.complete();
            } catch (Exception e) {
                log.error("fail to [MULTI_GET], keys: [{}], {}.", keys, StackTraceUtil.stackTrace(e));
            } finally {
                readLock.unlock();
            }
        });
    }

    @Override
    public Flux<Tuple<String, byte[]>> multiGetRaw(List<String> keys) {
        checkInit();

        if (CollectionUtils.isEmpty(keys)) {
            return Flux.empty();
        }

        return Flux.create(sink -> {
            List<byte[]> bKeys = keys.stream().map(this::toBKey).collect(Collectors.toList());

            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                List<byte[]> rawList = this.db.multiGetAsList(bKeys);

                int index = 0;
                for (byte[] value : rawList) {
                    String key = keys.get(index);
                    index++;

                    sink.next(new Tuple<>(key, value));
                }

                sink.complete();
            } catch (Exception e) {
                log.error("fail to [MULTI_GET], keys: [{}], {}.", keys, StackTraceUtil.stackTrace(e));
            } finally {
                readLock.unlock();
            }
        });
    }

    @Override
    public Mono<Void> put(String key, Object obj) {
        checkInit();

        return Mono.fromRunnable(() -> {
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                this.db.put(this.writeOptions, toBKey(key), JSON.writeBytes(obj));
            } catch (Exception e) {
                log.error("fail to [PUT], [{}, {}], {}.", key, obj, StackTraceUtil.stackTrace(e));
            } finally {
                readLock.unlock();
            }
        });
    }

    @Override
    public Mono<Void> put(Map<String, Object> kvs) {
        checkInit();

        if (CollectionUtils.isEmpty(kvs)) {
            return Mono.empty();
        }

        return Mono.fromRunnable(() -> {
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
                this.db.write(this.writeOptions, batch);
            } catch (Exception e) {
                log.error("fail to [PUT_LIST], {}, {}.", kvEntries, StackTraceUtil.stackTrace(e));
            } finally {
                readLock.unlock();
            }
        });
    }

    @Override
    public <T> Flux<Tuple<String, T>> scan(String startKey, String endKey, Class<T> type) {
        checkInit();

        return Flux.create(sink -> {
            int maxCount = Integer.MAX_VALUE;
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try (RocksIterator it = this.db.newIterator()) {
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
                    sink.next(new Tuple<>(toSKey(key), JSON.read(it.value(), type)));
                    it.next();
                }
                sink.complete();
            } catch (Exception e) {
                log.error("fail to [SCAN], range: ['[{}, {})'], {}.", startKey, endKey, StackTraceUtil.stackTrace(e));
            } finally {
                readLock.unlock();
            }
        });
    }

    @Override
    public Flux<Tuple<String, byte[]>> scanRaw(String startKey, String endKey) {
        checkInit();

        return Flux.create(sink -> {
            int maxCount = Integer.MAX_VALUE;
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try (RocksIterator it = this.db.newIterator()) {
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
                sink.complete();
            } catch (Exception e) {
                log.error("fail to [SCAN], range: ['[{}, {})'], {}.", startKey, endKey, StackTraceUtil.stackTrace(e));
            } finally {
                readLock.unlock();
            }
        });
    }

    @Override
    public Mono<Void> delete(String key) {
        checkInit();

        return Mono.fromRunnable(() -> {
            Lock readLock = this.readWriteLock.readLock();
            readLock.lock();
            try {
                this.db.delete(this.writeOptions, toBKey(key));
            } catch (Exception e) {
                log.error("fail to [DELETE], [{}], {}.", key, StackTraceUtil.stackTrace(e));
            } finally {
                readLock.unlock();
            }
        });
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
        Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            if (this.db == null) {
                return;
            }
            this.shutdownLock.setMaxPermits(0);
            closeRocksDB();
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

    private void closeRocksDB() {
        if (this.db != null) {
            this.db.close();
            this.db = null;
        }
    }
}
