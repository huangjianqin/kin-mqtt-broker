package org.kin.mqtt.broker.example.gossip;

import org.kin.mqtt.broker.cluster.standalone.EnableMqttBroker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@EnableMqttBroker
@SpringBootApplication
public class MqttBrokerGossipApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqttBrokerGossipApplication.class, args);
    }
}
