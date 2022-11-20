package org.kin.mqtt.broker.auth.http;

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
@ConditionalOnProperty("org.kin.mqtt.broker.auth.url")
@Configuration
@EnableConfigurationProperties(HttpAuthProperties.class)
public class HttpAuthAutoConfiguration {
    @Autowired
    private HttpAuthProperties properties;

    @Bean
    public AuthService httpAuthService() {
        return new HttpAuthService(properties.getUrl());
    }
}
