package org.kin.mqtt.broker.core.cluster;

import org.kin.mqtt.broker.core.session.MqttSessionReplica;
import reactor.core.publisher.Mono;

/**
 * mqtt session持久化api
 * kin mqtt broker不会负责主动清理过期session
 *
 * @author huangjianqin
 * @date 2023/4/18
 */
public interface MqttSessionStore {
    /**
     * 获取mqtt session副本
     *
     * @param clientId mqtt client id
     * @return mqtt session副本
     */
    Mono<MqttSessionReplica> get(String clientId);

    /**
     * 持久化mqtt session副本
     *
     * @param replica mqtt session副本
     * @return complete signal
     */
    Mono<Void> save(MqttSessionReplica replica);

    /**
     * 移除持久化mqtt session副本
     *
     * @param clientId mqtt client id
     * @return complete signal
     */
    Mono<Void> remove(String clientId);

    /**
     * 异步持久化mqtt session副本
     *
     * @param replica mqtt session副本
     */
    default void saveAsync(MqttSessionReplica replica) {
        save(replica).subscribe();
    }
}
