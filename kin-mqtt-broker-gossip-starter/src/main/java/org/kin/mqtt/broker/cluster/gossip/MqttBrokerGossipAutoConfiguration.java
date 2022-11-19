package org.kin.mqtt.broker.cluster.gossip;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConditionalOnProperty({"org.kin.mqtt.broker.gossip.port",
        "org.kin.mqtt.broker.gossip.seeds"})
@Configuration
@EnableConfigurationProperties(GossipProperties.class)
public class MqttBrokerGossipAutoConfiguration {
    @Autowired
    private GossipProperties gossipProperties;

    @Bean
    public GossipBrokerManager gossipBrokerManager() {
        return new GossipBrokerManager(gossipProperties);
    }
}
