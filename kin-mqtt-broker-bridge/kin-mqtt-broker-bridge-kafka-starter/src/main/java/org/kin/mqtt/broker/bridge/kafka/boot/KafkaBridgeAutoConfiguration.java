package org.kin.mqtt.broker.bridge.kafka.boot;

import org.kin.mqtt.broker.bridge.Bridge;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

/**
 * @author huangjianqin
 * @date 2022/11/22
 */
@ConditionalOnExpression("!'${spring.kafka.producer}'.isEmpty()")
@ConditionalOnBean(KafkaProperties.class)
@Configuration
public class KafkaBridgeAutoConfiguration {
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean(destroyMethod = "close")
    public ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate() {
        return new ReactiveKafkaProducerTemplate<>(SenderOptions.create(kafkaProperties.getProducer().buildProperties()));
    }

    /**
     * 默认只加载default的kafka bridge
     */
    @Bean
    public Bridge kafkaBridge(@Autowired ReactiveKafkaProducerTemplate<String, String> sender) {
        return new KafkaBridge(sender);
    }
}
