package org.kin.mqtt.broker.acl.db;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.kin.mqtt.broker.acl.AclAction;
import org.kin.mqtt.broker.acl.AclService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author huangjianqin
 * @date 2023/1/2
 */
public class DBAclService implements AclService {
    /** r2dbc连接池 */
    private final ConnectionFactory connectionFactory;

    public DBAclService(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public Mono<Boolean> checkPermission(String host, String clientId, String userName, String topicName, AclAction action) {
        return Mono.usingWhen(connectionFactory.create(),
                connection -> Flux.from(connection.createStatement("SELECT * FROM kin_mqtt_acl WHERE ip_addr = ? AND username = ? AND client_id = ? AND topic = ? AND access = ?")
                                .bind(0, host)
                                .bind(1, userName)
                                .bind(2, clientId)
                                .bind(3, topicName)
                                .bind(4, action.getFlag())
                                .execute())
                        .next()
                        .flatMap(result -> Mono.from(result.map((row, rowMetadata) -> row.get("allow", Integer.class) == 1))),
                Connection::close);
    }
}
