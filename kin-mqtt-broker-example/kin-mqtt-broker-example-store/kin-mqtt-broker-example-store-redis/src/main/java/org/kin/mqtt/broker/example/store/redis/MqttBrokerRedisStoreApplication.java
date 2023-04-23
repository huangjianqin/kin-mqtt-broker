package org.kin.mqtt.broker.example.store.redis;

import org.kin.mqtt.broker.cluster.standalone.EnableMqttBroker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author huangjianqin
 * @date 2023/4/22
 */
@EnableMqttBroker
@SpringBootApplication
public class MqttBrokerRedisStoreApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqttBrokerRedisStoreApplication.class, args);
    }
}