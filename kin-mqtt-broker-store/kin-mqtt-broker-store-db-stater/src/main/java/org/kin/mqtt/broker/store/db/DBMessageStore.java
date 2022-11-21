package org.kin.mqtt.broker.store.db;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.*;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.store.AbstractMessageStore;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

/**
 * 基于db存储retain和offline消息
 *
 * @author huangjianqin
 * @date 2022/11/20
 */
public final class DBMessageStore extends AbstractMessageStore {
    /** r2dbc连接池 */
    private final ConnectionPool connectionPool;

    public DBMessageStore(DBMessageStoreProperties properties) {
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

        this.connectionPool = new ConnectionPool(configuration);
    }

    @Override
    public void saveOfflineMessage(MqttMessageReplica replica) {
        Flux.usingWhen(connectionPool.create(),
                        connection -> Flux.from(
                                //保存offline消息
                                connection.createStatement("INSERT INTO kin_mqtt_broker_offline('client_id', 'topic', 'qos', 'retain', 'payload', 'create_time', 'properties')" +
                                                " values (?,?,?,?,?,?,?)")
                                        .bind(0, replica.getClientId())
                                        .bind(1, replica.getTopic())
                                        .bind(2, replica.getQos())
                                        .bind(3, replica.isRetain() ? 1 : 0)
                                        .bind(4, JSON.write(replica.getPayload()))
                                        .bind(5, replica.getTimestamp())
                                        .bind(6, JSON.write(replica.getProperties()))
                                        .execute()
                        ),
                        Connection::close)
                .subscribe();
    }

    @Nonnull
    @Override
    public Flux<MqttMessageReplica> getOfflineMessage(String clientId) {
        return Flux.usingWhen(connectionPool.create(),
                //根据mqtt client id查询offline消息
                connection -> Flux.from(connection.createStatement("SELECT * FROM kin_mqtt_broker_offline WHERE client_id = ?")
                                .bind(0, clientId)
                                .execute())
                        //结果转换
                        .flatMap(this::queryResult2MqttMessageReplica),
                Connection::close);
    }

    @Override
    public void saveRetainMessage(MqttMessageReplica replica) {
        byte[] payload = replica.getPayload();
        if (Objects.isNull(payload) || payload.length == 0) {
            //payload为空, 删除retain消息
            Flux.usingWhen(connectionPool.create(),
                            connection -> Flux.from(
                                    connection.createStatement("DELETE FROM kin_mqtt_broker_retain WHERE topic = ?")
                                            .bind(0, replica.getTopic())
                                            .execute()
                            ),
                            Connection::close)
                    .subscribe();
        } else {
            //替换retain消息
            Flux.usingWhen(connectionPool.create(),
                            connection -> Mono.from(connection.beginTransaction())
                                    .flatMapMany(v -> connection.createStatement("SELECT count(1) FROM kin_mqtt_broker_retain WHERE topic = ?")
                                            .bind(0, replica.getTopic())
                                            .execute())
                                    .single()
                                    .flatMapMany(r -> r.map((row, rowMetadata) -> row.get(0, Integer.class)))
                                    .single()
                                    .flatMapMany(count -> {
                                        Statement statement;
                                        if (count > 0) {
                                            //已经retain消息, 则update
                                            statement = connection.createStatement("UPDATE kin_mqtt_broker_retain SET 'client_id' = ?, 'topic' = ?, 'qos' = ?, 'retain' = ?, 'payload' = ?, 'create_time' = ?, 'properties' = ?" +
                                                            " WHERE 'topic' = ?")
                                                    .bind(7, replica.getTopic());
                                        } else {
                                            //没有retain消息, 则insert
                                            statement = connection.createStatement("INSERT INTO kin_mqtt_broker_retain('client_id', 'topic', 'qos', 'retain', 'payload', 'create_time', 'properties')" +
                                                    " values (?,?,?,?,?,?,?)");
                                        }

                                        return statement
                                                .bind(0, replica.getClientId())
                                                .bind(1, replica.getTopic())
                                                .bind(2, replica.getQos())
                                                .bind(3, replica.isRetain() ? 1 : 0)
                                                .bind(4, JSON.write(replica.getPayload()))
                                                .bind(5, replica.getTimestamp())
                                                .bind(6, JSON.write(replica.getProperties()))
                                                .execute();
                                    })

                            ,
                            Connection::close)
                    .subscribe();
        }
    }

    @Nonnull
    @Override
    public Flux<MqttMessageReplica> getRetainMessage(String topic) {
        return Flux.usingWhen(connectionPool.create(),
                connection -> Flux.from(
                                //全量拉取
                                connection.createStatement("SELECT * FROM kin_mqtt_broker_retain").execute())
                        .flatMap(r -> r.map((row, rowMetadata) -> row))
                        //topic匹配
                        .filter(row -> {
                            String queryTopic = row.get("topic", String.class);
                            if (StringUtils.isNotBlank(queryTopic)) {
                                return queryTopic.matches(toRegexTopic(topic));
                            }
                            return false;
                        })
                        //结果转换
                        .map(this::queryResult2MqttMessageReplica),
                Connection::close);
    }

    /**
     * sql查询结果转换成{@link  MqttMessageReplica}
     */
    private Publisher<MqttMessageReplica> queryResult2MqttMessageReplica(Result r) {
        return r.map((row, rowMetadata) -> queryResult2MqttMessageReplica(row));
    }

    /**
     * sql查询行记录转换成{@link  MqttMessageReplica}
     */
    @SuppressWarnings("ConstantConditions")
    private MqttMessageReplica queryResult2MqttMessageReplica(Row row) {
        MqttMessageReplica.Builder builder = MqttMessageReplica.builder();
        return builder.clientId(row.get("client_id", String.class))
                .topic(row.get("topic", String.class))
                .qos(row.get("qos", Integer.class))
                .setRetain(row.get("retain", Boolean.class))
                .payload(JSON.read(row.get("payload", String.class), byte[].class))
                .timestamp(row.get("create_time", Long.class))
                .properties(JSON.readMap(row.get("properties", String.class))
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())))
                .build();
    }

    @Override
    public void dispose() {
        connectionPool.dispose();
    }
}