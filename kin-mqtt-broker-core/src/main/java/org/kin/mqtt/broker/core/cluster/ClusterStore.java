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
    void init();

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
