package org.kin.mqtt.broker.store.db;

import org.kin.mqtt.broker.Constants;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConditionalOnProperty({Constants.STORE_PROPERTIES_PREFIX + ".db.driver",
        Constants.STORE_PROPERTIES_PREFIX + ".db.port",
        Constants.STORE_PROPERTIES_PREFIX + ".db.user",
        Constants.STORE_PROPERTIES_PREFIX + ".db.password"})
@Configuration
@EnableConfigurationProperties(DBMessageStoreProperties.class)
public class DBMessageStoreAutoConfiguration {
    @Autowired
    private DBMessageStoreProperties properties;

    @Bean(destroyMethod = "close")
    public MqttMessageStore dbMessageStore() {
        return new DBMessageStore(properties);
    }
}
