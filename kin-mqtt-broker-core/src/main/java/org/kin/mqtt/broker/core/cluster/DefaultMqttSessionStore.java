package org.kin.mqtt.broker.core.cluster;

import org.kin.mqtt.broker.core.session.MqttSessionReplica;
import reactor.core.publisher.Mono;

/**
 * 基于cluster store存储session持久化消息
 *
 * @author huangjianqin
 * @date 2023/4/18
 */
public class DefaultMqttSessionStore implements MqttSessionStore {
    /** cluster store */
    private final ClusterStore clusterStore;

    public DefaultMqttSessionStore(ClusterStore clusterStore) {
        this.clusterStore = clusterStore;
    }

    @Override
    public Mono<MqttSessionReplica> get(String clientId) {
        return clusterStore.get(ClusterStoreKeys.getSessionKey(clientId), MqttSessionReplica.class);
    }

    @Override
    public Mono<Void> save(MqttSessionReplica replica) {
        return clusterStore.put(ClusterStoreKeys.getSessionKey(replica.getClientId()), replica);
    }

    @Override
    public Mono<Void> remove(String clientId) {
        return clusterStore.delete(ClusterStoreKeys.getSessionKey(clientId));
    }
}
