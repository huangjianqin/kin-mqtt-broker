package org.kin.mqtt.broker.bridge.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import org.kin.framework.utils.JSON;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.bridge.NoErrorBridge;
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
public final class RabbitMQBridge extends NoErrorBridge {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQBridge.class);

    /** rabbit mq sender */
    private final Sender sender;

    public RabbitMQBridge(SenderOptions senderOptions) {
        this(DEFAULT_NAME, senderOptions);
    }

    public RabbitMQBridge(String name, SenderOptions senderOptions) {
        super(name);
        sender = RabbitFlux.createSender(senderOptions);
    }

    public static SenderOptions getDefaultSenderOptions(int port) {
        return getDefaultSenderOptions("localhost", port, null, null);
    }

    public static SenderOptions getDefaultSenderOptions(String host, int port) {
        return getDefaultSenderOptions(host, port, null, null);
    }

    public static SenderOptions getDefaultSenderOptions(String host, int port, String user, String password) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setTopologyRecoveryEnabled(true);
        connectionFactory.setNetworkRecoveryInterval(3000);
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        if (StringUtils.isNotBlank(user)) {
            connectionFactory.setUsername(user);
        }
        if (StringUtils.isNotBlank(password)) {
            connectionFactory.setPassword(password);
        }
        return new SenderOptions()
                .connectionFactory(connectionFactory);
    }

    @Override
    protected Mono<Void> transmit0(ContextAttrs attrs) {
        String queue = attrs.rmAttr(BridgeAttrNames.RABBITMQ_QUEUE);

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
    public BridgeType type() {
        return BridgeType.RABBITMQ;
    }

    @Override
    public void close() {
        sender.close();
    }
}
