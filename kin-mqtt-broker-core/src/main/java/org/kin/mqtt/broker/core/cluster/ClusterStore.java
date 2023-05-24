package org.kin.mqtt.broker.core.cluster;

import org.kin.framework.collection.Tuple;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/**
 * 集群数据访问接口
 *
 * @author huangjianqin
 * @date 2023/5/19
 */
public interface ClusterStore {
    /**
     * 初始化cluster store
     */
    Mono<Void> init();

    /**
     * 获取数据
     *
     * @param key  key
     * @param type 数据类型
     */
    <T> Mono<T> get(String key, Class<T> type);

    /**
     * 获取原始数据
     *
     * @param key key
     */
    Mono<byte[]> get(String key);

    /**
     * 批量获取数据
     *
     * @param keys key数组
     * @param type 数据类型
     */
    <T> Flux<Tuple<String, T>> multiGet(List<String> keys, Class<T> type);

    /**
     * 批量获取原始数据
     *
     * @param keys key数组
     */
    Flux<Tuple<String, byte[]>> multiGetRaw(List<String> keys);

    /**
     * 存储数据
     *
     * @param key key
     * @param obj value
     */
    Mono<Void> put(String key, Object obj);

    /**
     * 批量存储数据
     *
     * @param kvs 数据map
     */
    Mono<Void> put(Map<String, Object> kvs);

    /**
     * 扫描[{@code startKey}, {@code endKey}]之间所有key的kv对
     * !!!可能会有重复key的kv对出现
     *
     * @param startKey start key
     * @param endKey   end key
     * @param type     value类型
     * @return Flux
     */
    <T> Flux<Tuple<String, T>> scan(String startKey, String endKey, Class<T> type);

    /**
     * 扫描{@code startKey}后面所有key的kv对
     * !!!可能会有重复key的kv对出现
     *
     * @param startKey start key
     * @param type     value类型
     * @return Flux
     */
    default <T> Flux<Tuple<String, T>> scan(String startKey, Class<T> type) {
        return scan(startKey, null, type);
    }

    /**
     * 扫描[{@code startKey}, {@code endKey}]之间所有key的kv对, value为原始byte[]
     * !!!可能会有重复key的kv对出现
     *
     * @param startKey start key
     * @param endKey   end key
     * @return Flux
     */
    Flux<Tuple<String, byte[]>> scanRaw(String startKey, String endKey);

    /**
     * 扫描{@code startKey}后面所有key的kv对, value为原始byte[]
     * !!!可能会有重复key的kv对出现
     *
     * @param startKey start key
     * @return Flux
     */
    default Flux<Tuple<String, byte[]>> scanRaw(String startKey) {
        return scanRaw(startKey, null);
    }

    /**
     * 移除指定key的value
     *
     * @param key key
     * @return complete signal
     */
    Mono<Void> delete(String key);

    /**
     * 添加replicator
     *
     * @param nodeAddress replicator node address
     */
    void addReplicator(String nodeAddress);

    /**
     * 移除replicator
     *
     * @param nodeAddress replicator node address
     */
    void removeReplicator(String nodeAddress);

    /**
     * shutdown
     */
    void shutdown();
}
