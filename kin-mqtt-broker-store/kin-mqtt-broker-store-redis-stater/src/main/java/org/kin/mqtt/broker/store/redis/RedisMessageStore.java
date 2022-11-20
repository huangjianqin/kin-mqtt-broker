package org.kin.mqtt.broker.store.redis;

import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.store.AbstractMessageStore;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * 基于redis存储retain和offline消息
 *
 * @author huangjianqin
 * @date 2022/11/20
 */
public final class RedisMessageStore extends AbstractMessageStore {
    /** redis中保存离线消息的key */
    private static final String OFFLINE_MESSAGE_KEY_PREFIX = "KinMQTTBroker:offline:message:";
    /** redis中保存retain消息的key */
    private static final String RETAIN_MESSAGE_KEY_PREFIX = "KinMQTTBroker:retain:message:";

    /** redis client */
    private final ReactiveRedisTemplate<String, Object> template;

    public RedisMessageStore(ReactiveRedisTemplate<String, Object> template) {
        this.template = template;
    }

    /**
     * 获取保存离线消息的redis key
     */
    private String getOfflineMessageKey(String clientId) {
        return OFFLINE_MESSAGE_KEY_PREFIX + clientId;
    }

    @Override
    public void saveOfflineMessage(MqttMessageReplica replica) {
        String clientId = replica.getClientId();
        template.opsForList().rightPush(getOfflineMessageKey(clientId), replica).subscribe();
    }

    @Nonnull
    @Override
    public Flux<MqttMessageReplica> getOfflineMessage(String clientId) {
        String key = getOfflineMessageKey(clientId);
        return template.opsForList()
                //取所有元素
                .range(key, 0, -1)
                .cast(MqttMessageReplica.class)
                .collectList()
                .flatMapMany(list -> template.opsForList().trim(key, 0, list.size() - 1)
                        .thenMany(Flux.fromIterable(list)));
    }

    /**
     * 获取保存retain消息的redis key
     */
    private String getRetainMessageKey(String topic) {
        return OFFLINE_MESSAGE_KEY_PREFIX + topic;
    }

    @Override
    public void saveRetainMessage(MqttMessageReplica replica) {
        String topic = replica.getTopic();
        byte[] payload = replica.getMessage();
        String key = getRetainMessageKey(topic);
        if (Objects.isNull(payload) || payload.length == 0) {
            //删除retain消息
            template.opsForValue().delete(key).subscribe();
        } else {
            //替换retain消息
            template.opsForValue().set(key, replica).subscribe();
        }
    }

    @Nonnull
    @Override
    public Flux<MqttMessageReplica> getRetainMessage(String topic) {
        return template.keys(RETAIN_MESSAGE_KEY_PREFIX + "*")
                //替换前缀
                .map(key -> key.replaceAll(RETAIN_MESSAGE_KEY_PREFIX, ""))
                //过滤不匹配的topic
                .filter(tp -> tp.matches(toRegexTopic(topic)))
                //redis get对应topic的retain消息
                .flatMap(tp -> template.opsForValue()
                        .get(getRetainMessageKey(tp))
                        .cast(MqttMessageReplica.class));
    }
}
