package org.kin.mqtt.broker.bridge.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.IgnoreErrorBridge;
import org.kin.mqtt.broker.bridge.definition.RabbitMQBridgeDefinition;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import java.nio.charset.StandardCharsets;

/**
 * 基于reactor-rabbitmq
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class RabbitMQBridge extends IgnoreErrorBridge {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQBridge.class);

    /** rabbit mq sender */
    private final Sender sender;

    public RabbitMQBridge(String name, SenderOptions senderOptions) {
        super(name);
        sender = RabbitFlux.createSender(senderOptions);
    }

    public RabbitMQBridge(RabbitMQBridgeDefinition definition) {
        super(definition.getName());
        sender = RabbitFlux.createSender(toSenderOptions(definition));
    }

    public static SenderOptions getDefaultSenderOptions(int port) {
        return getDefaultSenderOptions("localhost", port, null, null);
    }

    public static SenderOptions getDefaultSenderOptions(String host, int port) {
        return getDefaultSenderOptions(host, port, null, null);
    }

    public static SenderOptions getDefaultSenderOptions(String host, int port, String user, String password) {
        return toSenderOptions(RabbitMQBridgeDefinition.builder()
                .host(host)
                .port(port)
                .userName(user)
                .password(password)
                .build());
    }

    private static SenderOptions toSenderOptions(RabbitMQBridgeDefinition definition) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setTopologyRecoveryEnabled(true);
        connectionFactory.setConnectionTimeout(definition.getConnectionTimeout());
        connectionFactory.setNetworkRecoveryInterval(definition.getReconnectInterval());
        connectionFactory.setChannelRpcTimeout(definition.getRpcTimeout());
        connectionFactory.setHost(definition.getHost());
        connectionFactory.setPort(definition.getPort());
        if (StringUtils.isNotBlank(definition.getUserName())) {
            connectionFactory.setUsername(definition.getUserName());
        }
        if (StringUtils.isNotBlank(definition.getPassword())) {
            connectionFactory.setPassword(definition.getPassword());
        }

        //共享connection
        Mono<Connection> connectionMono = Mono.fromCallable(connectionFactory::newConnection);

        return new SenderOptions()
                .connectionMono(connectionMono)
                .channelPool(ChannelPoolFactory.createChannelPool(connectionMono, new ChannelPoolOptions().maxCacheSize(definition.getPoolSize())));
    }

    @Override
    protected Mono<Void> transmit0(ContextAttrs attrs) {
        String queue = attrs.removeAttr(BridgeAttrNames.RABBITMQ_QUEUE);

        return Mono.just(attrs)
                .flatMap(a -> sender.declareQueue(QueueSpecification.queue(queue)))
                .flatMapMany(declareOk -> sender.sendWithPublishConfirms(Mono.just(
                        new OutboundMessage("", queue, JSON.write(attrs).getBytes(StandardCharsets.UTF_8)))))
                .doOnNext(result -> {
                    if (result.isAck()) {
                        log.debug("rabbitMQ message '{}' sent successfully, queue={} timestamp={}", attrs, queue, System.currentTimeMillis());
                    } else {
                        throw new IllegalStateException("rabbitMQ message send fail, rabbitMQ broker doesn't ack");
                    }
                })
                .then();
    }

    @Override
    public void close() {
        sender.close();
    }
}
