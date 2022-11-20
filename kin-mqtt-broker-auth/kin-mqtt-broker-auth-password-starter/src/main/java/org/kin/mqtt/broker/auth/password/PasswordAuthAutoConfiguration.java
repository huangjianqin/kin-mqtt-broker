package org.kin.mqtt.broker.auth.password;

import org.kin.mqtt.broker.auth.AuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConditionalOnProperty({"org.kin.mqtt.broker.auth.username",
        "org.kin.mqtt.broker.auth.password"})
@Configuration
@EnableConfigurationProperties(PasswordAuthProperties.class)
public class PasswordAuthAutoConfiguration {
    @Autowired
    private PasswordAuthProperties properties;

    @Bean
    public AuthService passwordAuthService() {
        return new PasswordAuthService(properties);
    }
}
