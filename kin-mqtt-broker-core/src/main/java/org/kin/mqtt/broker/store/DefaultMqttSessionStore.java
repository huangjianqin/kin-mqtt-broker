package org.kin.mqtt.broker.store;

import org.kin.mqtt.broker.core.session.MqttSessionReplica;
import reactor.core.publisher.Mono;

/**
 * 默认不支持mqtt session持久化
 *
 * @author huangjianqin
 * @date 2023/4/18
 */
public class DefaultMqttSessionStore implements MqttSessionStore {
    /** 单例 */
    public static final MqttSessionStore INSTANCE = new DefaultMqttSessionStore();

    private DefaultMqttSessionStore() {
    }

    @Override
    public Mono<MqttSessionReplica> get(String clientId) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> save(MqttSessionReplica replica) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> remove(String clientId) {
        return Mono.empty();
    }
}
