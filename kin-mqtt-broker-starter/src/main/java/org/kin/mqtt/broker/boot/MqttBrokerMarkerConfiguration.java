package org.kin.mqtt.broker.boot;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 注入{@link Marker}bean, 进而触发加载{@link MqttBrokerStandAloneAutoConfiguration}
 *
 * @author huangjianqin
 * @date 2022/11/19
 */
@Configuration(proxyBeanMethods = false)
public class MqttBrokerMarkerConfiguration {
    @Bean
    public MqttBrokerMarkerConfiguration.Marker mqttBrokerMarker() {
        return new MqttBrokerMarkerConfiguration.Marker();
    }

    static class Marker {
        Marker() {
        }
    }
}
