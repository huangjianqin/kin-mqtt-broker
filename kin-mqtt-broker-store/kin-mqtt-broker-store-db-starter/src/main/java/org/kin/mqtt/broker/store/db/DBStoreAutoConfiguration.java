package org.kin.mqtt.broker.store.db;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.ConnectionFactory;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConditionalOnBean(ConnectionPool.class)
@ConditionalOnMissingBean(MqttMessageStore.class)
@Configuration
public class DBStoreAutoConfiguration {

    /**
     * 支持使用自定义配置, {@link DBMessageStoreProperties}或者Spring Data R2DBC(3.0集成在Spring Data Relational), 二选一
     */
    @Bean(destroyMethod = "close")
    public MqttMessageStore dbMqttMessageStore(@Autowired ConnectionFactory connectionFactory) {
        return new DBMqttMessageStore(connectionFactory);
    }
}
