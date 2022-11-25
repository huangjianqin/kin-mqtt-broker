package org.kin.mqtt.broker.cluster.standalone;

import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.bridge.Bridge;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.core.Interceptor;
import org.kin.mqtt.broker.core.MqttBroker;
import org.kin.mqtt.broker.core.MqttBrokerBootstrap;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.List;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConditionalOnBean(MqttBrokerMarkerConfiguration.Marker.class)
@Configuration
@EnableConfigurationProperties(MqttBrokerProperties.class)
public class MqttBrokerStandAloneAutoConfiguration {
    @Autowired
    private MqttBrokerProperties mqttBrokerProperties;

    @Bean(destroyMethod = "close")
    public MqttBroker mqttBroker(@Autowired(required = false) List<Interceptor> interceptors,
                                 @Autowired(required = false) AuthService authService,
                                 @Autowired(required = false) BrokerManager brokerManager,
                                 @Autowired(required = false) MqttMessageStore messageStore,
                                 @Autowired(required = false) List<Bridge> bridges,
                                 @Autowired(required = false) AclService aclService) {
        MqttBrokerBootstrap bootstrap = MqttBrokerBootstrap.create();
        bootstrap.port(mqttBrokerProperties.getPort())
                .messageMaxSize(mqttBrokerProperties.getMessageMaxSize());

        if (mqttBrokerProperties.isOverWebsocket()) {
            bootstrap.wsPort(mqttBrokerProperties.getWsPort())
                    .wsPath(mqttBrokerProperties.getWsPath());
        }

        if (mqttBrokerProperties.isSsl()) {
            bootstrap.ssl(true)
                    .certFile(new File(mqttBrokerProperties.getCertFile()))
                    .certKeyFile(new File(mqttBrokerProperties.getCertKeyFile()))
                    .caFile(new File(mqttBrokerProperties.getCaFile()));
        }

        if (CollectionUtils.isNonEmpty(interceptors)) {
            bootstrap.interceptors(interceptors);
        }

        if (Objects.nonNull(authService)) {
            bootstrap.authService(authService);
        }

        if (Objects.nonNull(brokerManager)) {
            bootstrap.brokerManager(brokerManager);
        }

        if (Objects.nonNull(messageStore)) {
            bootstrap.messageStore(messageStore);
        }

        if (CollectionUtils.isNonEmpty(bridges)) {
            bootstrap.bridges(bridges);
        }

        if (Objects.nonNull(aclService)) {
            bootstrap.aclService(aclService);
        }

        return bootstrap.start();
    }
}
