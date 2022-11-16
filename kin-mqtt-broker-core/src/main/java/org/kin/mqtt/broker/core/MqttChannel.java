package org.kin.mqtt.broker.core;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.core.will.Will;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ReactorNetty;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mqtt client连接
 *
 * @author huangjianqin
 * @date 2022/11/13
 */
public class MqttChannel {
    private static final Logger log = LoggerFactory.getLogger(MqttChannel.class);

    /** 用于控制建立connection后, client还不发送connect消息, 则broker主动关闭connection. 默认10s */
    private static final int DEFER_CLOSE_WITHOUT_CONNECT_MESSAGE_SECONDS = 10;

    /** broker context */
    private final MqttBrokerContext brokerContext;
    /** mqtt client connection */
    private final Connection connection;
    /** mqtt client id */
    protected String clientId;
    /** mqtt client connect时间, 校验通过后的时间 */
    private long connectTime;
    /** 是否是会话持久化 */
    private boolean persistent;
    /** mqtt client user name */
    private String userName;
    /** mqtt channel status */
    private volatile ChannelStatus status = ChannelStatus.INIT;
    /** 遗愿 */
    private Will will;
    /** 用于控制建立connection后, client还不发送connect消息, 则broker主动关闭connection */
    private Disposable deferCloseWithoutConnMsgDisposable;
    /** mqtt client 订阅 */
    private final Set<TopicSubscription> subscriptions = new CopyOnWriteArraySet<>();
    /** mqtt response 消息id */
    private final AtomicInteger messageIdGenerator = new AtomicInteger();
    /**
     * at least once消息缓存
     *
     * @see io.netty.handler.codec.mqtt.MqttQoS#AT_LEAST_ONCE
     */
    private final Map<Integer, MqttPublishMessage> qos2MessageCache = new ConcurrentHashMap<>();

    public MqttChannel(MqttBrokerContext brokerContext, Connection connection) {
        this.brokerContext = brokerContext;
        this.connection = connection;
    }

    /**
     * 往指定mqtt channel发送mqtt消息
     *
     * @param mqttMessage mqtt消息
     * @param retry       是否重试
     * @return complete signal
     */
    public Mono<Void> sendMessage(MqttMessage mqttMessage, boolean retry) {
        log.debug("channel {} send message {}", getConnection(), mqttMessage);
        if (retry) {
            //Increase the reference count of bytebuf, and the reference count of retrybytebuf is 2
            //mqttChannel.write() method releases a reference count.
            MqttMessageType mqttMessageType = mqttMessage.fixedHeader().messageType();
            //待发送的mqtt消息
            MqttMessage reply = getReplyMqttMessage(mqttMessage);

            Runnable retryTask = () -> write(Mono.just(reply)).subscribe();
            Runnable cleaner = () -> ReactorNetty.safeRelease(reply);

            RetryService retryService = brokerContext.getRetryService();
            //开启retry task, 最大重试次数为5, 间隔3s
            long uuid = generateUuid(mqttMessageType, MqttMessageUtils.getMessageId(mqttMessage));
            retryService.execRetry(new PublishRetry(uuid, retryTask, cleaner, retryService));

            return write(Mono.just(mqttMessage)).then();
        } else {
            return write(Mono.just(mqttMessage));
        }
    }

    /**
     * 获取要发送的mqtt消息
     *
     * @param mqttMessage mqtt消息
     * @return 要发送的mqtt消息
     */
    private MqttMessage getReplyMqttMessage(MqttMessage mqttMessage) {
        if (mqttMessage instanceof MqttPublishMessage) {
            // TODO: 2022/11/14
            return ((MqttPublishMessage) mqttMessage).copy().retain(PublishRetry.DEFAULT_MAX_RETRY_TIMES);
        } else {
            return mqttMessage;
        }
    }

    /**
     * 添加订阅
     *
     * @param subscription 订阅信息
     */
    public void addSubscription(TopicSubscription subscription) {
        subscriptions.add(subscription);
    }

    /**
     * 移除订阅
     *
     * @param subscription 订阅信息
     */
    public void removeSubscription(TopicSubscription subscription) {
        subscriptions.remove(subscription);
    }

    /**
     * 回写mqtt消息
     *
     * @param messageMono mqtt消息
     * @return complete signal
     */
    private Mono<Void> write(Mono<MqttMessage> messageMono) {
        if (this.connection.channel().isActive() && this.connection.channel().isWritable()) {
            return connection.outbound().sendObject(messageMono).then();
        } else {
            return Mono.empty();
        }
    }

    /**
     * 生成唯一ID, 用于retry或者其他用途
     *
     * @param type      mqtt消息类型
     * @param messageId mqtt消息package id
     * @return 唯一ID, 即32位connection hashcode + 28位mqtt消息类型 + 4位mqtt消息package id
     */
    public long generateUuid(MqttMessageType type, Integer messageId) {
        return (long) connection.channel().hashCode() << 32 | (long) type.value() << 28 | messageId << 4 >>> 4;
    }

    /**
     * @return 下一消息id
     */
    public int nextMessageId() {
        int value;
        while (qos2MessageCache.containsKey(value = messageIdGenerator.incrementAndGet())) {
            if (value >= 65535) {
                //消息id有最大限制
                synchronized (this) {
                    value = messageIdGenerator.incrementAndGet();
                    if (value >= 65535) {
                        messageIdGenerator.set(0);
                    } else {
                        break;
                    }
                }
            }
        }
        return value;
    }

    /**
     * 缓存qos2消息
     *
     * @param messageId      消息id
     * @param publishMessage mqtt publish消息
     * @return complete signal
     */
    public Mono<Void> cacheQos2Message(int messageId, MqttPublishMessage publishMessage) {
        return Mono.fromRunnable(() -> qos2MessageCache.put(messageId, publishMessage));
    }

    /**
     * 是否有qos2消息缓存
     *
     * @param messageId 消息id
     * @return 有qos2消息缓存
     */
    public boolean existQos2Message(int messageId) {
        return qos2MessageCache.containsKey(messageId);
    }

    /**
     * 移除qos2消息缓存
     *
     * @param messageId 消息id
     * @return 缓存的qos2消息
     */
    public Optional<MqttPublishMessage> removeQos2Message(int messageId) {
        return Optional.ofNullable(qos2MessageCache.remove(messageId));
    }

    /**
     * @return mqtt channel是否在线
     */
    public boolean isOnline() {
        return status == ChannelStatus.ONLINE;
    }

    /**
     * @return 是否是集群broker channel实例
     */
    public boolean isBrokerChannel() {
        return false;
    }

    /**
     * 用于控制建立connection后, client还不发送connect消息, 则broker主动关闭connection
     *
     * @return this
     */
    public MqttChannel deferCloseWithoutConnMsg() {
        // registry tcp close event
        deferCloseWithoutConnMsgDisposable = Mono.fromRunnable(() -> {
            if (!connection.isDisposed()) {
                connection.dispose();
            }
        }).delaySubscription(Duration.ofSeconds(DEFER_CLOSE_WITHOUT_CONNECT_MESSAGE_SECONDS)).subscribe();
        return this;
    }

    /**
     * 连接成功后的处理
     */
    public void onConnectSuccess(String clientId, MqttConnectVariableHeader variableHeader, MqttConnectPayload payload) {
        //关闭延迟关闭没有发起connect的mqtt client
        if (deferCloseWithoutConnMsgDisposable != null && !deferCloseWithoutConnMsgDisposable.isDisposed()) {
            deferCloseWithoutConnMsgDisposable.dispose();
        }

        this.clientId = clientId;
        connectTime = System.currentTimeMillis();
        persistent = !variableHeader.isCleanSession();
        status = ChannelStatus.ONLINE;
        userName = payload.userName();

        //keepalive
        //mqtt client 空闲, broker关闭mqtt client连接
        connection.onReadIdle((long) variableHeader.keepAliveTimeSeconds() * 1000, this::close0);

        //will
        if (variableHeader.isWillFlag()) {
            will = Will.builder()
                    .setRetain(variableHeader.isWillRetain())
                    .topic(payload.willTopic())
                    .message(payload.willMessageInBytes())
                    .qoS(MqttQoS.valueOf(variableHeader.willQos()))
                    .build();

            afterDispose(this::handleWillAfterClose);
        }

        brokerContext.getChannelManager().register(clientId, this);
        afterDispose(this::close0);

        // TODO: 2022/11/15
//        metricManager.getMetricRegistry().getMetricCounter(CounterType.CONNECT).increment();
//        afterDispose(channel -> metricManager.getMetricRegistry().getMetricCounter(CounterType.CONNECT).decrement());
//
//        eventRegistry.registry(Event.CONNECT, mqttChannel, message, mqttReceiveContext);
    }

    /**
     * connection close主要逻辑
     */
    private void close0() {
        log.info("{}: mqtt channel closed, {}", Thread.currentThread().getName(), this);

        offline();
        if (!persistent) {
            brokerContext.getTopicManager().removeAllSubscriptions(this);
            brokerContext.getChannelManager().remove(clientId);
        }
        will = null;

        // TODO: 2022/11/15
//        eventRegistry.registry(Event.CLOSE, mqttChannel, null, mqttReceiveContext);
//        metricManager.getMetricRegistry().getMetricCounter(CounterType.CLOSE_EVENT).increment();
    }

    /**
     * mqtt channel close
     *
     * @return close complete signal
     */
    public Mono<Void> close() {
        return Mono.fromRunnable(() -> {
            qos2MessageCache.clear();
            if (!persistent) {
                subscriptions.clear();
            }
            offline();
            if (!connection.isDisposed()) {
                connection.dispose();
            }
        });
    }

    /**
     * 绑定mqtt client close之后的操作
     *
     * @param runnable mqtt client close之后的操作
     */
    private void afterDispose(Runnable runnable) {
        connection.onDispose(runnable::run);
    }

    /**
     * mqtt client close之后的遗愿处理
     */
    private void handleWillAfterClose() {
        TopicManager topicManager = brokerContext.getTopicManager();
        topicManager.getSubscriptions(will.getTopic(), will.getQoS())
                .forEach(subscription -> {
                    MqttChannel channel = subscription.getMqttChannel();

                    ByteBuf byteBuf = connection.channel().alloc().directBuffer();
                    byteBuf.writeBytes(will.getMessage());
                    sendMessage(MqttMessageUtils.createPublish(false,
                                    subscription.getQoS(),
                                    subscription.getQoS() == MqttQoS.AT_MOST_ONCE ? 0 : channel.nextMessageId(),
                                    will.getTopic(),
                                    byteBuf),
                            subscription.getQoS().value() > 0)
                            .subscribe();
                });
    }

    /**
     * 将状态设置为{@link ChannelStatus#OFFLINE}
     */
    private void offline() {
        status = ChannelStatus.OFFLINE;
    }

    //getter
    public Set<TopicSubscription> getSubscriptions() {
        return subscriptions;
    }

    public Connection getConnection() {
        return connection;
    }

    public String getClientId() {
        return clientId;
    }

    public long getConnectTime() {
        return connectTime;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public String getUserName() {
        return userName;
    }
}
