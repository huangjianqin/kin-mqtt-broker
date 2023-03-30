package org.kin.mqtt.broker.auth.db;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.kin.framework.utils.MD5;
import org.kin.mqtt.broker.auth.AuthService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author huangjianqin
 * @date 2023/1/2
 */
public class DBAuthService implements AuthService {
    /** r2dbc连接池 */
    private final ConnectionFactory connectionFactory;

    public DBAuthService(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public Mono<Boolean> auth(String userName, String password) {
        return Mono.usingWhen(connectionFactory.create(),
                connection -> Flux.from(connection.createStatement("SELECT * FROM kin_mqtt_user WHERE username = ?")
                                .bind(0, userName)
                                .execute())
                        .next()
                        //MD5(MD5(password) + salt)
                        .flatMap(result -> Mono.from(result.map((row, rowMetadata) -> {
                            String dbPassword = row.get("password", String.class);
                            String salt = row.get("salt", String.class);
                            return MD5.current().digestAsHex(password + salt).equals(dbPassword);
                        }))),
                Connection::close);
    }
}
