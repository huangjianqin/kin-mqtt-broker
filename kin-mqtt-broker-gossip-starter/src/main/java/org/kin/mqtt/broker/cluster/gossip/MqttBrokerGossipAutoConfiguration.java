package org.kin.mqtt.broker.cluster.gossip;

import org.kin.mqtt.broker.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConditionalOnProperty({Constants.GOSSIP_PROPERTIES_PREFIX + ".port",
        Constants.GOSSIP_PROPERTIES_PREFIX + ".seeds"})
@Configuration
@EnableConfigurationProperties(GossipProperties.class)
public class MqttBrokerGossipAutoConfiguration {
    @Autowired
    private GossipConfig gossipConfig;

    @Bean
    public GossipBrokerManager gossipBrokerManager() {
        return new GossipBrokerManager(gossipConfig);
    }
}
