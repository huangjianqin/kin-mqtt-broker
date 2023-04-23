package org.kin.mqtt.broker.store.redis;

import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.mqtt.broker.store.MqttSessionStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveRedisTemplate;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConditionalOnExpression("!'${spring.redis}'.isEmpty()")
@Configuration
public class RedisStoreAutoConfiguration {
    @ConditionalOnMissingBean(MqttMessageStore.class)
    @Bean
    public MqttMessageStore redisMqttMessageStore(@Autowired ReactiveRedisTemplate<String, String> template) {
        return new RedisMqttMessageStore(template);
    }

    @ConditionalOnMissingBean(MqttSessionStore.class)
    @Bean
    public MqttSessionStore redisMqttSessionStore(@Autowired ReactiveRedisTemplate<String, String> template) {
        return new RedisMqttSessionStore(template);
    }
}
