package org.kin.mqtt.broker.core;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.Timeout;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashSet;
import org.kin.mqtt.broker.cluster.event.SubscriptionsRemoveEvent;
import org.kin.mqtt.broker.core.message.MqttMessageUtils;
import org.kin.mqtt.broker.core.topic.TopicManager;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.core.will.Will;
import org.kin.mqtt.broker.event.MqttClientDisConnEvent;
import org.kin.mqtt.broker.event.MqttClientRegisterEvent;
import org.kin.mqtt.broker.event.MqttClientUnregisterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ReactorNetty;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    private Connection connection;
    /** mqtt channel hash code */
    private int channelHashCode;
    /** mqtt client host */
    protected String host;
    /** mqtt client id */
    protected String clientId;
    /** mqtt client connect时间, 校验通过后的时间 */
    private long connectTime;
    /** 连接断开时, 是否是清除会话 */
    private boolean cleanSession;
    /** 会话过期时间(秒) */
    private int sessionExpiryInterval;
    /** mqtt client user name */
    private String userName;
    /** mqtt channel status */
    private volatile ChannelStatus status = ChannelStatus.INIT;
    /** 接收端愿意同时处理的QoS为1和2的PUBLISH消息最大数量 */
    private int receiveMaximum;
    /** 当前已接收但未处理的PUBLISH消息数量 */
    private AtomicInteger receiveNum;

    /** 遗愿 */
    private Will will;
    /** 用于控制建立connection后, client还不发送connect消息, 则broker主动关闭connection */
    private Disposable deferCloseWithoutConnMsgDisposable;
    /** mqtt client 订阅 */
    private Set<TopicSubscription> subscriptions;
    /** mqtt response 消息id */
    private AtomicInteger messageIdGenerator;
    /**
     * at least once消息缓存
     *
     * @see io.netty.handler.codec.mqtt.MqttQoS#AT_LEAST_ONCE
     */
    private Map<Integer, MqttPublishMessage> qos2MessageCache;
    /** session过期定时任务 */
    private Timeout sessionExpiryTimeout;
    /** 延迟处理will的Disposable实例, 可能为null, 即无延迟处理will */
    private Disposable delayHandleWillDisposable;
    /** key -> topic别名alias, value -> 真实topic */
    private NonBlockingHashMap<Integer, String> alias2TopicName;

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
        log.debug("channel {} send {} message", getConnection(), mqttMessage.fixedHeader().messageType());
        if (retry) {
            //发送qos > 0的publish消息
            onSendQos1Message();

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
     * 发送publish消息时触发, 记录对端已收到但未发送响应的QoS大于0的PUBLISH消息数量
     */
    private void onSendQos1Message() {
        if (receiveMaximum <= 0) {
            return;
        }

        if (receiveNum.incrementAndGet() >= receiveMaximum) {
            //已接收到但未处理publish消息
            //broker直接断开连接
            close().subscribe();
            //然后抛异常
            throw new IllegalStateException(String.format("mqtt channel '%s' rate limited!", clientId));
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
        if (Objects.nonNull(connection) &&
                this.connection.channel().isActive() &&
                this.connection.channel().isWritable()) {
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
        return (long) channelHashCode << 32 | (long) type.value() << 28 | messageId << 4 >>> 4;
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
     * @return mqtt channel是否离线
     */
    public boolean isOffline() {
        return status == ChannelStatus.OFFLINE;
    }

    /**
     * @return 是否是虚拟mqtt channel实例, 即来自于集群, 规则引擎触发的mqtt消息处理
     */
    public boolean isVirtualChannel() {
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
            //此时不为null
            if (!connection.isDisposed()) {
                connection.dispose();
            }
        }).delaySubscription(Duration.ofSeconds(DEFER_CLOSE_WITHOUT_CONNECT_MESSAGE_SECONDS)).subscribe();
        return this;
    }

    /**
     * 接受connect消息并成功通过校验后, 执行channel初始化
     */
    @SuppressWarnings("unchecked")
    public void onConnect(String clientId, MqttConnectVariableHeader variableHeader,
                          MqttConnectPayload payload) {
        //关闭延迟关闭没有发起connect的mqtt client
        if (deferCloseWithoutConnMsgDisposable != null && !deferCloseWithoutConnMsgDisposable.isDisposed()) {
            deferCloseWithoutConnMsgDisposable.dispose();
            //release, 后面没用了释放对象
            deferCloseWithoutConnMsgDisposable = null;
        }

        //初始化字段, 因为离线后, 新连接会创建mqtt channel, 选择不在定义时初始化字段,
        //则是在session持久化场景下可以减少新对象分配(新mqtt channel对象仅用于恢复旧mqtt channel, 然后会被抛弃)
        //此时不为null
        this.host = connection.address().toString().split(":")[0];
        this.channelHashCode = connection.channel().hashCode();
        this.clientId = clientId;
        connectTime = System.currentTimeMillis();
        cleanSession = variableHeader.isCleanSession();
        status = ChannelStatus.ONLINE;
        userName = payload.userName();
        MqttProperties properties = variableHeader.properties();
        MqttProperties.MqttProperty<Integer> sessionExpiryIntervalProp = properties.getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
        if (Objects.nonNull(sessionExpiryIntervalProp)) {
            updateSessionExpiryInterval(sessionExpiryIntervalProp.value());
        }
        MqttProperties.MqttProperty<Integer> receiveMaximumProp = properties.getProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value());
        if (Objects.nonNull(receiveMaximumProp)) {
            this.receiveMaximum = receiveMaximumProp.value();
            this.receiveNum = new AtomicInteger();
        }
        subscriptions = new NonBlockingHashSet<>();
        messageIdGenerator = new AtomicInteger();
        qos2MessageCache = new NonBlockingHashMap<>();
        alias2TopicName = new NonBlockingHashMap<>();

        //keepalive
        //mqtt client 空闲, broker关闭mqtt client连接
        //此时不为null
        connection.onReadIdle((long) variableHeader.keepAliveTimeSeconds() * 1000, this::close0);

        //will
        if (variableHeader.isWillFlag()) {
            int willDelay = 0;
            MqttProperties.MqttProperty<Integer> willDelayIntervalProp = properties.getProperty(MqttProperties.MqttPropertyType.WILL_DELAY_INTERVAL.value());
            if (Objects.nonNull(willDelayIntervalProp)) {
                willDelay = willDelayIntervalProp.value();
            }
            will = Will.builder()
                    .setRetain(variableHeader.isWillRetain())
                    .topic(payload.willTopic())
                    .message(payload.willMessageInBytes())
                    .qoS(MqttQoS.valueOf(variableHeader.willQos()))
                    .delay(willDelay)
                    .build();
            //注册dispose后will逻辑
            afterDispose(this::handleWillAfterClose);
        }

        //注册dispose逻辑
        afterDispose(this::close0);

        //register channel
        if (brokerContext.getChannelManager().register(clientId, this)) {
            brokerContext.broadcastEvent(new MqttClientRegisterEvent(this));
        }
    }

    /**
     * 基于新connection恢复原channel状态
     */
    @SuppressWarnings("unchecked")
    public void onReconnect(MqttChannel newMqttChannel, MqttConnectVariableHeader variableHeader,
                            MqttConnectPayload payload) {
        //防止自动断开新connection
        Disposable deferCloseWithoutConnMsgDisposable = newMqttChannel.deferCloseWithoutConnMsgDisposable;
        if (deferCloseWithoutConnMsgDisposable != null && !deferCloseWithoutConnMsgDisposable.isDisposed()) {
            deferCloseWithoutConnMsgDisposable.dispose();
        }

        //取消session过期
        cancelSessionExpiryTimeout();
        //取消will延迟处理
        tryCancelDelayHandleWillDisposable();

        //初始化字段
        //替换connection
        this.connection = newMqttChannel.connection;
        //此时不为null
        this.host = connection.address().toString().split(":")[0];
        this.channelHashCode = connection.channel().hashCode();
        cleanSession = variableHeader.isCleanSession();
        status = ChannelStatus.ONLINE;
        userName = payload.userName();
        MqttProperties properties = variableHeader.properties();
        MqttProperties.MqttProperty<Integer> sessionExpiryIntervalProp = properties.getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
        if (Objects.nonNull(sessionExpiryIntervalProp)) {
            updateSessionExpiryInterval(sessionExpiryIntervalProp.value());
        }
        MqttProperties.MqttProperty<Integer> receiveMaximumProp = properties.getProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value());
        if (Objects.nonNull(receiveMaximumProp)) {
            this.receiveMaximum = receiveMaximumProp.value();
            this.receiveNum = new AtomicInteger();
        }
        //keepalive
        //mqtt client 空闲, broker关闭mqtt client连接
        //此时不为null
        connection.onReadIdle((long) variableHeader.keepAliveTimeSeconds() * 1000, this::close0);

        //will
        if (variableHeader.isWillFlag()) {
            will = Will.builder()
                    .setRetain(variableHeader.isWillRetain())
                    .topic(payload.willTopic())
                    .message(payload.willMessageInBytes())
                    .qoS(MqttQoS.valueOf(variableHeader.willQos()))
                    .build();
            //注册dispose后will逻辑
            afterDispose(this::handleWillAfterClose);
        }

        //注册dispose逻辑
        afterDispose(this::close0);
    }

    /**
     * connection close主要逻辑
     */
    private void close0() {
        if (isOffline()) {
            return;
        }

        log.info("mqtt channel closed, {}", this);

        releaseSessionLessField();
        SubscriptionsRemoveEvent subscriptionsRemoveEvent = SubscriptionsRemoveEvent.of(subscriptions.stream().map(TopicSubscription::getTopic).collect(Collectors.toList()));
        if (cleanSession) {
            // TODO: 2022/12/23 持久化session支持存库和集群共享, 是不是得全部释放, 然后加载进来时, 自动注册
            //非持久化session
            cleanSession(false);
            brokerContext.broadcastEvent(new MqttClientUnregisterEvent(this));
        } else {
            if (sessionExpiryInterval > 0) {
                long expiryTime = connectTime + TimeUnit.SECONDS.toMillis(sessionExpiryInterval) - System.currentTimeMillis();
                if (expiryTime > 0) {
                    //调度session过期
                    sessionExpiryTimeout = brokerContext.getBsTimer().newTimeout(t -> cleanSession(false), expiryTime, TimeUnit.MILLISECONDS);
                } else {
                    //已过期
                    cleanSession(false);
                }
            }
        }
        brokerContext.broadcastEvent(new MqttClientDisConnEvent(this));
        //无论session是否持久化, 都集群广播topic订阅移除事件
        brokerContext.broadcastClusterEvent(subscriptionsRemoveEvent);
    }

    /**
     * 清空与会话无关的字段实例, 仅保留session状态相关的实例
     */
    private void releaseSessionLessField() {
        connection = null;
        userName = null;
        will = null;
        deferCloseWithoutConnMsgDisposable = null;
        receiveNum = null;
    }

    /**
     * 清空会话状态
     *
     * @param replaceOldChannelFlag 持久化session断开连接后, 重连成功并且不设置持久化才设置该标识,
     *                              表示新mqtt channel直接替代旧mqtt channel, 而不是恢复其状态
     */
    public void cleanSession(boolean replaceOldChannelFlag) {
        if (replaceOldChannelFlag) {
            //也算是重连, 取消will延迟处理
            tryCancelDelayHandleWillDisposable();
        }
        //取消channel注册
        brokerContext.getChannelManager().remove(clientId);
        //取消订阅
        //!!会清空MqttChannel.subscriptions
        brokerContext.getTopicManager().removeAllSubscriptions(this);
    }

    /**
     * mqtt channel close
     *
     * @return close complete signal
     */
    public Mono<Void> close() {
        return Mono.fromRunnable(() -> {
            if (isOffline()) {
                return;
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
        //此时不为null
        connection.onDispose(runnable::run);
    }

    /**
     * mqtt client close之后的will处理
     */
    private void handleWillAfterClose() {
        int delay = will.getDelay();
        if (delay > 0) {
            //will延迟
            delayHandleWillDisposable = Mono.delay(Duration.ofSeconds(delay))
                    .then(Mono.fromRunnable(this::handleWill))
                    .subscribe();
        } else {
            //无延迟
            handleWill();
        }
    }

    /**
     * will处理
     */
    private void handleWill() {
        TopicManager topicManager = brokerContext.getTopicManager();
        topicManager.getSubscriptions(will.getTopic(), will.getQoS(), this)
                .forEach(subscription -> {
                    MqttChannel channel = subscription.getMqttChannel();

                    //此时不为null
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
     * 会话重连, 则取消will延迟处理
     */
    private void tryCancelDelayHandleWillDisposable() {
        if (Objects.nonNull(delayHandleWillDisposable)) {
            delayHandleWillDisposable.dispose();
        }
    }

    /**
     * 将状态设置为{@link ChannelStatus#OFFLINE}
     */
    private void offline() {
        status = ChannelStatus.OFFLINE;
    }

    /**
     * 更新session过期时间, 目前仅在处理connect和disconnect消息时调用
     *
     * @param sessionExpiryInterval session过期时间
     */
    public void updateSessionExpiryInterval(int sessionExpiryInterval) {
        this.sessionExpiryInterval = sessionExpiryInterval;
    }

    /**
     * 判断session是否过期
     *
     * @return session是否过期
     */
    public boolean isSessionExpiry() {
        return isOffline() && System.currentTimeMillis() >= connectTime + TimeUnit.SECONDS.toMillis(sessionExpiryInterval);
    }

    /**
     * 取消session过期定时任务
     */
    private void cancelSessionExpiryTimeout() {
        if (Objects.isNull(sessionExpiryTimeout)) {
            return;
        }

        sessionExpiryTimeout.cancel();
        sessionExpiryTimeout = null;
    }

    /**
     * 注册topic别名
     *
     * @param alias     topic别名
     * @param topicName 真实topic
     */
    public void registerTopicAlias(int alias, String topicName) {
        alias2TopicName.put(alias, topicName);
    }

    /**
     * 根据topic别名获取真实topic
     *
     * @param alias topic别名
     */
    @Nullable
    public String getTopicByAlias(int alias) {
        return alias2TopicName.get(alias);
    }

    /**
     * 接收到publish的响应消息, 即PUBACK, PUBCOMP或PUBREC
     */
    public void onRecPubRespMessage() {
        if (receiveMaximum <= 0) {
            return;
        }

        if (receiveNum.decrementAndGet() < 0) {
            log.error("mqtt channel '%s' receiveNum decrease to negative");
        }
    }

    /**
     * 过滤已注册的topic订阅, 同时使用新订阅qos替换旧订阅qos
     */
    public Set<TopicSubscription> filterRegisteredTopicSubscriptions(Set<TopicSubscription> subscriptions) {
        Map<String, TopicSubscription> topic2qos = subscriptions.stream().collect(Collectors.toMap(TopicSubscription::getTopic, ts -> ts));
        for (TopicSubscription subscription : this.subscriptions) {
            TopicSubscription newSubscription = topic2qos.remove(subscription.getRawTopic());
            if (Objects.nonNull(newSubscription)) {
                subscription.setQoS(newSubscription.getQoS());
            }
        }

        return new HashSet<>(topic2qos.values());
    }


    //getter
    public MqttBrokerContext getBrokerContext() {
        return brokerContext;
    }

    public String getHost() {
        return host;
    }

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

    public boolean isCleanSession() {
        return cleanSession;
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MqttChannel)) return false;
        MqttChannel that = (MqttChannel) o;
        return Objects.equals(connection, that.connection) && Objects.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connection, clientId);
    }

    @Override
    public String toString() {
        return "MqttChannel{" +
                "connection=" + connection +
                ", channelHashCode=" + channelHashCode +
                ", host='" + host + '\'' +
                ", clientId='" + clientId + '\'' +
                ", connectTime=" + connectTime +
                ", cleanSession=" + cleanSession +
                ", sessionExpiryInterval=" + sessionExpiryInterval +
                ", userName='" + userName + '\'' +
                ", status=" + status +
                ", will=" + will +
                ", subscriptions=" + subscriptions.stream().map(ts -> ts.getTopic() + ":" + ts.getQoS()).collect(Collectors.toList()) +
                ", messageIdGenerator=" + messageIdGenerator +
                '}';
    }
}
