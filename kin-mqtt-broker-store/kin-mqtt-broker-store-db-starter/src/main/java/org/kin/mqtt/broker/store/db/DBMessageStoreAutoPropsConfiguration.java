package org.kin.mqtt.broker.store.db;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.kin.mqtt.broker.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

/**
 * 如果存在配置, 加载{@link  DBMessageStoreProperties}和自定义{@link ConnectionPool} bean
 *
 * @author huangjianqin
 * @date 2022/11/25
 */
@ConditionalOnProperty({Constants.STORE_PROPERTIES_PREFIX + ".db.driver",
        Constants.STORE_PROPERTIES_PREFIX + ".db.port",
        Constants.STORE_PROPERTIES_PREFIX + ".db.user",
        Constants.STORE_PROPERTIES_PREFIX + ".db.password"})
@Configuration
@EnableConfigurationProperties(DBMessageStoreProperties.class)
public class DBMessageStoreAutoPropsConfiguration {

    @Bean(destroyMethod = "dispose")
    public ConnectionPool connectionFactory(@Autowired DBMessageStoreProperties properties) {
        ConnectionFactory pooledConnectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, properties.getDriver())
                .option(HOST, properties.getHost())
                .option(PORT, properties.getPort())
                .option(USER, properties.getUser())
                .option(PASSWORD, properties.getPassword())
                .option(DATABASE, properties.getDatabase())
                .build());

        //连接池配置
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(pooledConnectionFactory)
                //最大空闲时间
                .maxIdleTime(Duration.ofMinutes(5))
                //最小空闲连接数量
                .minIdle(2)
                //最大连接数
                .maxSize(20)
                .build();

        return new ConnectionPool(configuration);
    }
}
