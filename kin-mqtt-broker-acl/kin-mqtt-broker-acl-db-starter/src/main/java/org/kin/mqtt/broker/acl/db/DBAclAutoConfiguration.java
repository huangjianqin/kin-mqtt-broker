package org.kin.mqtt.broker.acl.db;

import io.r2dbc.spi.ConnectionFactory;
import org.kin.mqtt.broker.acl.AclService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2023/1/2
 */
@Configuration
public class DBAclAutoConfiguration {

    @Bean
    public AclService dbAuthService(@Autowired ConnectionFactory connectionFactory) {
        return new DBAclService(connectionFactory);
    }
}
