package org.kin.mqtt.broker.store.db;

import io.r2dbc.spi.*;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.store.AbstractMqttMessageStore;
import org.kin.mqtt.broker.utils.TopicUtils;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 基于db存储retain和offline消息
 *
 * @author huangjianqin
 * @date 2022/11/20
 */
public class DBMqttMessageStore extends AbstractMqttMessageStore {
    private static final Logger log = LoggerFactory.getLogger(DBMqttMessageStore.class);
    /** r2dbc连接池 */
    private final ConnectionFactory connectionFactory;

    public DBMqttMessageStore(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void saveOfflineMessage(String clientId, MqttMessageReplica replica) {
        Mono.usingWhen(connectionFactory.create(),
                        connection -> Mono.from(connection.beginTransaction())
                                //保存offline消息
                                .flatMap(v ->
                                        Mono.from(connection.createStatement("INSERT INTO kin_mqtt_broker_offline('client_id', 'topic', 'qos', 'retain', 'payload', 'create_time', 'properties')" +
                                                        " values (?,?,?,?,?,?,?)")
                                                .bind(0, clientId)
                                                .bind(1, replica.getTopic())
                                                .bind(2, replica.getQos())
                                                .bind(3, replica.isRetain() ? 1 : 0)
                                                .bind(4, JSON.write(replica.getPayload()))
                                                .bind(5, replica.getRecTime())
                                                .bind(6, JSON.write(replica.getProperties()))
                                                .execute()))
                                .thenEmpty(connection.commitTransaction())
                                .onErrorResume(t -> {
                                    log.error("save offline message error, ", t);
                                    return Mono.from(connection.rollbackTransaction());
                                }),
                        Connection::close)
                .subscribe();
    }

    @Override
    public void saveOfflineMessages(String clientId, Collection<MqttMessageReplica> replicas) {
        if (CollectionUtils.isEmpty(replicas)) {
            return;
        }

        Mono.usingWhen(connectionFactory.create(),
                        connection -> Mono.from(connection.beginTransaction())
                                //保存offline消息
                                .flatMap(v -> {
                                    String sql = "INSERT INTO kin_mqtt_broker_offline('client_id', 'topic', 'qos', 'retain', 'payload', 'create_time', 'properties') values (?,?,?,?,?,?,?)";
                                    for (int i = 0; i < replicas.size() - 1; i++) {
                                        sql += ", (?,?,?,?,?,?,?)";
                                    }

                                    Statement statement = connection.createStatement(sql);
                                    int idx = 0;
                                    for (MqttMessageReplica replica : replicas) {
                                        statement.bind(idx++, clientId)
                                                .bind(idx++, replica.getTopic())
                                                .bind(idx++, replica.getQos())
                                                .bind(idx++, replica.isRetain() ? 1 : 0)
                                                .bind(idx++, JSON.write(replica.getPayload()))
                                                .bind(idx++, replica.getRecTime())
                                                .bind(idx++, JSON.write(replica.getProperties()));
                                    }

                                    return Mono.from(statement.execute());
                                })
                                .thenEmpty(connection.commitTransaction())
                                .onErrorResume(t -> {
                                    log.error("batch save offline message error, ", t);
                                    return Mono.from(connection.rollbackTransaction());
                                }),
                        Connection::close)
                .subscribe();
    }

    @Nonnull
    @Override
    public Flux<MqttMessageReplica> getAndRemoveOfflineMessage(String clientId) {
        return Flux.usingWhen(connectionFactory.create(),
                //根据mqtt client id查询offline消息
                connection -> {
                    Flux<? extends Result> resultFlux = Flux.from(connection.createStatement("SELECT * FROM kin_mqtt_broker_offline WHERE client_id = ?")
                            .bind(0, clientId)
                            .execute());
                    //异步delete
                    resultFlux.flatMap(r -> r.map((row, rowMetadata) -> row.get("client_id", Long.class)))
                            .collectList()
                            .doOnNext(this::delInvalidOfflineMessage)
                            .subscribe();

                    //结果转换
                    return resultFlux.flatMap(this::queryResult2MqttMessageReplica);
                },
                Connection::close);
    }

    /**
     * 删除已消费的离线消息
     *
     * @param ids 离线消息记录主键id
     */
    private void delInvalidOfflineMessage(List<Long> ids) {
        Mono.usingWhen(connectionFactory.create(),
                        connection -> Mono.from(connection.beginTransaction())
                                .flatMap(v ->
                                        Mono.from(connection.createStatement("DELETE FROM kin_mqtt_broker_offline WHERE id in (?)")
                                                .bind(0, StringUtils.mkString(ids))
                                                .execute()))
                                .thenEmpty(connection.commitTransaction())
                                .onErrorResume(t -> {
                                    log.error("delete offline message error, ", t);
                                    return Mono.from(connection.rollbackTransaction());
                                }),
                        Connection::close)
                .subscribe();
    }


    @Override
    public void saveRetainMessage(MqttMessageReplica replica) {
        byte[] payload = replica.getPayload();
        if (Objects.isNull(payload) || payload.length == 0) {
            //payload为空, 删除retain消息
            Mono.usingWhen(connectionFactory.create(),
                            connection -> Mono.from(connection.beginTransaction())
                                    .flatMap(v ->
                                            Mono.from(connection.createStatement("DELETE FROM kin_mqtt_broker_retain WHERE topic = ?")
                                                    .bind(0, replica.getTopic())
                                                    .execute()))
                                    .thenEmpty(connection.commitTransaction())
                                    .onErrorResume(t -> {
                                        log.error("delete retain message error, ", t);
                                        return Mono.from(connection.rollbackTransaction());
                                    }),
                            Connection::close)
                    .subscribe();
        } else {
            //替换retain消息
            Mono.usingWhen(connectionFactory.create(),
                            connection -> Mono.from(connection.beginTransaction())
                                    .flatMap(v -> Mono.from(connection.createStatement("SELECT count(1) FROM kin_mqtt_broker_retain WHERE topic = ?")
                                            .bind(0, replica.getTopic())
                                            .execute()))
                                    .flatMap(r -> Mono.from(r.map((row, rowMetadata) -> row.get(0, Integer.class))))
                                    .flatMap(count -> {
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

                                        return Mono.from(statement
                                                .bind(0, replica.getClientId())
                                                .bind(1, replica.getTopic())
                                                .bind(2, replica.getQos())
                                                .bind(3, replica.isRetain() ? 1 : 0)
                                                .bind(4, JSON.write(replica.getPayload()))
                                                .bind(5, replica.getRecTime())
                                                .bind(6, JSON.write(replica.getProperties()))
                                                .execute());
                                    })
                                    .thenEmpty(connection.commitTransaction())
                                    .onErrorResume(t -> {
                                        log.error("update retain message error, ", t);
                                        return Mono.from(connection.rollbackTransaction());
                                    })
                            ,
                            Connection::close)
                    .subscribe();
        }
    }

    @Nonnull
    @Override
    public Flux<MqttMessageReplica> getRetainMessage(String topic) {
        return Flux.usingWhen(connectionFactory.create(),
                connection -> Flux.from(
                                //全量拉取
                                connection.createStatement("SELECT * FROM kin_mqtt_broker_retain").execute())
                        .flatMap(r -> r.map((row, rowMetadata) -> row))
                        //topic匹配
                        .filter(row -> {
                            String queryTopic = row.get("topic", String.class);
                            if (StringUtils.isNotBlank(queryTopic)) {
                                return queryTopic.matches(TopicUtils.toRegexTopic(topic));
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
                .recTime(row.get("create_time", Long.class))
                .properties(JSON.readMap(row.get("properties", String.class))
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())))
                .build();
    }
}
