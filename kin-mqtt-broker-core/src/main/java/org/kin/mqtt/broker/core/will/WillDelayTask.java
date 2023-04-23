package org.kin.mqtt.broker.core.will;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.kin.mqtt.broker.core.MqttBrokerContext;
import org.kin.mqtt.broker.core.MqttSession;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.session.MqttSessionReplica;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.mqtt.broker.store.MqttSessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * will消息延迟处理
 *
 * @author huangjianqin
 * @date 2023/4/22
 */
public class WillDelayTask implements TimerTask {
    private static final Logger log = LoggerFactory.getLogger(WillDelayTask.class);

    /** mqtt broker context */
    private final MqttBrokerContext brokerContext;
    /** 所属mqtt client */
    private final String clientId;
    /** will消息payload bytebuf allocator */
    private final ByteBufAllocator allocator;
    /** will消息 */
    private final Will will;

    public WillDelayTask(MqttBrokerContext brokerContext,
                         String clientId,
                         ByteBufAllocator allocator,
                         Will will) {
        this.brokerContext = brokerContext;
        this.clientId = clientId;
        this.allocator = allocator;
        this.will = will;
    }

    @Override
    public void run(Timeout timeout) {
        MqttSessionStore sessionStore = brokerContext.getSessionStore();
        sessionStore.get(clientId)
                .onErrorResume(e -> {
                    log.error("get session(clientId={}) from {} error", clientId, sessionStore.getClass().getSimpleName(), e);
                    //查询不到(持久化)session相关信息, 则认为session过期, 直接执行will
                    return Mono.empty();
                })
                //session未过期
                .flatMap(this::runWhenTimeout)
                //session已过期
                .switchIfEmpty(Mono.fromRunnable(this::runNow))
                .subscribe();
    }

    /**
     * 延迟时间后执行
     */
    private Mono<Void> runWhenTimeout(MqttSessionReplica sessionReplica) {
        if (!brokerContext.getBrokerId().equals(sessionReplica.getBrokerId()) &&
                sessionReplica.isValid()) {
            //mqtt client在session过期前成功重连到其他broker
            return Mono.empty();
        }

        return Mono.fromRunnable(this::runNow);
    }

    /**
     * 立即执行
     */
    public void runNow() {
        TopicManager topicManager = brokerContext.getTopicManager();
        MqttMessageStore messageStore = brokerContext.getMessageStore();
        //广播will消息
        topicManager.getSubscriptions(will.getTopic(), will.getQos())
                .forEach(subscription -> {
                    MqttSession session = subscription.getMqttSession();

                    //此时不为null
                    ByteBuf byteBuf = allocator.directBuffer();
                    byteBuf.writeBytes(will.getMessage());
                    subscription.getMqttSession()
                            .sendMessage(MqttMessageHelper.createPublish(false,
                                            subscription.getQos(),
                                            will.isRetain(),
                                            subscription.getQos() == MqttQoS.AT_MOST_ONCE ? 0 : session.nextMessageId(),
                                            will.getTopic(),
                                            byteBuf,
                                            subscription.isRetainAsPublished()),
                                    subscription.getQos().value() > 0)
                            .subscribe();
                });

        //存储retain will消息
        if (will.isRetain()) {
            long now = System.currentTimeMillis();
            MqttMessageReplica willMessageReplica = MqttMessageReplica.builder()
                    .brokerId(brokerContext.getBrokerId())
                    .clientId(clientId)
                    .topic(will.getTopic())
                    .setRetain(will.isRetain())
                    .qos(will.getQos().value())
                    .payload(will.getMessage())
                    .recTime(now)
                    .expireTime(now + will.getExpiryInterval())
                    .build();
            messageStore.saveRetainMessage(willMessageReplica);
        }
    }
}
