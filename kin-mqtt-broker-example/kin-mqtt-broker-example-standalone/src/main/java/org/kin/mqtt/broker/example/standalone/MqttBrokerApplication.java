package org.kin.mqtt.broker.example.standalone;

import org.kin.mqtt.broker.cluster.standalone.EnableMqttBroker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@EnableMqttBroker
@SpringBootApplication
public class MqttBrokerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqttBrokerApplication.class, args);
    }
}
