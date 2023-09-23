package org.kin.mqtt.broker.bridge.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.NetUtils;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeConfiguration;
import org.kin.mqtt.broker.bridge.NamedBridge;
import org.kin.mqtt.broker.rule.ContextAttrs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import java.nio.charset.StandardCharsets;

import static org.kin.mqtt.broker.bridge.rabbitmq.RabbitMQBridgeConstants.*;

/**
 * 基于reactor-rabbitmq
 *
 * @author huangjianqin
 * @date 2022/11/22
 */
public class RabbitMQBridge extends NamedBridge {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQBridge.class);

    /** rabbit mq sender */
    private final Sender sender;

    public RabbitMQBridge(String name, SenderOptions senderOptions) {
        super(name);
        sender = RabbitFlux.createSender(senderOptions);
    }

    public RabbitMQBridge(BridgeConfiguration config) {
        super(config.getName());
        sender = RabbitFlux.createSender(toSenderOptions(config));
    }

    private static SenderOptions toSenderOptions(BridgeConfiguration config) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setTopologyRecoveryEnabled(true);
        connectionFactory.setConnectionTimeout(config.getInt(CONNECTION_TIMEOUT_KEY, DEFAULT_CONNECTION_TIMEOUT));
        connectionFactory.setNetworkRecoveryInterval(config.getInt(RECONNECT_INTERVAL_KEY, DEFAULT_RECONNECT_INTERVAL));
        connectionFactory.setChannelRpcTimeout(config.getInt(RPC_TIMEOUT_KEY, DEFAULT_RPC_TIMEOUT));
        connectionFactory.setHost(config.get(HOST_KEY, NetUtils.getLocalhost4Ip()));
        connectionFactory.setPort(config.getInt(PORT_KEY, DEFAULT_PORT));
        String userName = config.get(USER_NAME_KEY);
        if (StringUtils.isNotBlank(userName)) {
            connectionFactory.setUsername(userName);
        }
        String password = config.get(USER_NAME_KEY);
        if (StringUtils.isNotBlank(password)) {
            connectionFactory.setPassword(password);
        }

        //共享connection
        Mono<Connection> connectionMono = Mono.fromCallable(connectionFactory::newConnection);

        return new SenderOptions()
                .connectionMono(connectionMono)
                .channelPool(ChannelPoolFactory.createChannelPool(connectionMono,
                        new ChannelPoolOptions()
                                .maxCacheSize(config.getInt(POOL_SIZE_KEY, DEFAULT_POOL_SIZE))));
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
