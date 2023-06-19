package org.kin.mqtt.broker.boot;

import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.ServerCustomizer;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.core.Interceptor;
import org.kin.mqtt.broker.core.MqttBroker;
import org.kin.mqtt.broker.core.MqttBrokerBootstrap;
import org.kin.mqtt.broker.core.MqttMessageSender;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/19
 */
@ConditionalOnBean(MqttBrokerMarkerConfiguration.Marker.class)
@Configuration
@EnableConfigurationProperties(MqttBrokerProperties.class)
public class MqttBrokerAutoConfiguration {
    @Autowired
    private MqttBrokerProperties mqttBrokerProperties;

    @Bean(destroyMethod = "close")
    public MqttBroker mqttBroker(@Autowired ApplicationContext context,
                                 @Autowired(required = false) List<Interceptor> interceptors,
                                 @Autowired(required = false) AuthService authService,
                                 @Autowired(required = false) MqttMessageStore messageStore,
                                 @Autowired(required = false) AclService aclService,
                                 @Autowired(required = false) ServerCustomizer serverCustomizer) {
        MqttBrokerBootstrap bootstrap = MqttBrokerBootstrap.create(mqttBrokerProperties);

        if (CollectionUtils.isNonEmpty(interceptors)) {
            bootstrap.interceptors(interceptors);
        }

        if (Objects.nonNull(authService)) {
            bootstrap.authService(authService);
        }

        if (Objects.nonNull(messageStore)) {
            bootstrap.messageStore(messageStore);
        }

        bootstrap.rules(getRuleDefinitions());
        if (Objects.nonNull(aclService)) {
            bootstrap.aclService(aclService);
        }

        if (Objects.nonNull(serverCustomizer)) {
            bootstrap.serverCustomizer(serverCustomizer);
        }

        @SuppressWarnings("rawtypes")
        Collection<MqttEventConsumer> consumers = context.getBeansOfType(MqttEventConsumer.class).values();
        if (CollectionUtils.isNonEmpty(consumers)) {
            bootstrap.eventConsumers(consumers);
        }

        bootstrap.bridges(mqttBrokerProperties.getBridges()
                .stream()
                .map(MqttBrokerProperties.BridgeDefinition::toBridgeDefinition)
                .collect(Collectors.toList()));

        return bootstrap.start();
    }

    private List<org.kin.mqtt.broker.rule.RuleDefinition> getRuleDefinitions() {
        return mqttBrokerProperties.getRules().stream().map(MqttBrokerProperties.RuleDefinition::toRuleDefinition).collect(Collectors.toList());
    }

    @Bean
    public MqttMessageSender mqttMessageSender(@Autowired MqttBroker mqttBroker) {
        return mqttBroker.getMqttMessageSender();
    }
}
