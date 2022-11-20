package org.kin.mqtt.broker.store.redis;

import org.kin.mqtt.broker.store.MqttMessageStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConditionalOnExpression("!'${spring.redis}'.isEmpty()")
@Configuration
public class RedisMessageStoreAutoConfiguration {
    @Bean
    public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(@Autowired LettuceConnectionFactory connectionFactory) {
        StringRedisSerializer keySerializer = new StringRedisSerializer();
        RedisSerializer<Object> valueSerializer = genericJackson2JsonRedisSerializer();
        ReactiveRedisTemplate<String, Object> redisTemplate = new ReactiveRedisTemplate<>(connectionFactory,
                RedisSerializationContext.<String, Object>newSerializationContext()
                        .key(keySerializer)
                        .value(valueSerializer)
                        .hashKey(keySerializer)
                        .hashValue(valueSerializer)
                        .build());
        return redisTemplate;
    }

    @Bean
    public RedisSerializer<Object> genericJackson2JsonRedisSerializer() {
        //会自动序列化类型信息进入json, 然后反序列化时, 转换成对应对象
        //感觉原始json更好, 省去类型兼容问题, 也减少空间消耗, 但是需要开发者根据实际开发自定义组件, 方便开发
        return new GenericJackson2JsonRedisSerializer();
    }

    @Bean
    public MqttMessageStore redisMessageStore(@Autowired ReactiveRedisTemplate<String, Object> template) {
        return new RedisMessageStore(template);
    }
}
