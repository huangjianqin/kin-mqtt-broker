package org.kin.mqtt.broker.store.redis;

import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.core.session.MqttSessionReplica;
import org.kin.mqtt.broker.store.MqttSessionStore;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;

/**
 * 基于redis存储session持久化消息
 *
 * @author huangjianqin
 * @date 2023/4/18
 */
public class RedisMqttSessionStore implements MqttSessionStore {
    /** redis中保存session的key */
    private static final String SESSION_KEY = "KinMQTTBroker:session:%s";

    /** redis client */
    private final ReactiveRedisTemplate<String, String> template;

    public RedisMqttSessionStore(ReactiveRedisTemplate<String, String> template) {
        this.template = template;
    }

    /**
     * 获取保存session的redis key
     *
     * @param clientId mqtt session client id
     * @return 保存session的redis key
     */
    private static String getSessionKey(String clientId) {
        return String.format(SESSION_KEY, clientId);
    }

    @Override
    public Mono<MqttSessionReplica> get(String clientId) {
        return template.opsForValue()
                .get(getSessionKey(clientId))
                .flatMap(v -> {
                    if (Objects.nonNull(v)) {
                        return Mono.just(JSON.read(v, MqttSessionReplica.class));
                    } else {
                        return Mono.empty();
                    }
                });
    }

    @Override
    public Mono<Void> save(MqttSessionReplica replica) {
        String sessionKey = getSessionKey(replica.getClientId());

        //设置session过期时间
        Mono<Boolean> expireMono = Mono.empty();
        long expiryInternal = replica.getExpiryInternal();
        if (expiryInternal > 0) {
            expireMono = template.expire(sessionKey, Duration.ofMillis(expiryInternal));
        }

        return template.opsForValue()
                .set(sessionKey, JSON.write(replica))
                .then(expireMono)
                .then();
    }

    @Override
    public Mono<Void> remove(String clientId) {
        return template.delete(getSessionKey(clientId)).then();
    }
}
