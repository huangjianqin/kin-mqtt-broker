package org.kin.mqtt.broker.auth.db;

import io.r2dbc.spi.ConnectionFactory;
import org.kin.mqtt.broker.auth.AuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2023/1/2
 */
@Configuration
public class DBAuthAutoConfiguration {

    @Bean
    public AuthService dbAuthService(@Autowired ConnectionFactory connectionFactory) {
        return new DBAuthService(connectionFactory);
    }
}
