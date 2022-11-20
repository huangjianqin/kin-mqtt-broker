package org.kin.mqtt.broker.auth.user;

import org.kin.mqtt.broker.auth.AuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConditionalOnExpression("!'${org.kin.mqtt.broker.auth.users}'.isEmpty()")
@Configuration
@EnableConfigurationProperties(UserAuthProperties.class)
public class UserAuthAutoConfiguration {
    @Autowired
    private UserAuthProperties properties;

    @Bean
    public AuthService userAuthService() {
        return new UserAuthService(properties);
    }
}
