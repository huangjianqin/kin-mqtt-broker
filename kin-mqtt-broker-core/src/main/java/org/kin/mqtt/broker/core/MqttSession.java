package org.kin.mqtt.broker.core;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashSet;
import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.event.*;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.message.MqttMessageHelper;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.kin.mqtt.broker.core.message.MqttQos2PubMessage;
import org.kin.mqtt.broker.core.retry.PublishRetry;
import org.kin.mqtt.broker.core.retry.RetryService;
import org.kin.mqtt.broker.core.session.MqttSessionReplica;
import org.kin.mqtt.broker.core.topic.TopicSubscription;
import org.kin.mqtt.broker.core.topic.TopicSubscriptionReplica;
import org.kin.mqtt.broker.core.will.Will;
import org.kin.mqtt.broker.core.will.WillDelayTask;
import org.kin.mqtt.broker.store.MqttMessageStore;
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
public class MqttSession {
    private static final Logger log = LoggerFactory.getLogger(MqttSession.class);

    /** 用于控制建立connection后, client还不发送connect消息, 则broker主动关闭connection. 默认10s */
    private static final int DEFER_CLOSE_WITHOUT_CONNECT_MESSAGE_SECONDS = 10;

    /** broker context */
    private final MqttBrokerContext brokerContext;
    /** mqtt client connection */
    private Connection connection;
    /** mqtt channel hash code */
    private int channelHashCode;
    /** mqtt client host */
    private String host;
    /** mqtt client id */
    private String clientId;
    /**
     * mqtt client 首次connect时间
     * 持久化session在后续connect中并不会更新这个值, 该值表示该session第一次connect成功的时间
     */
    private long firstConnectTime;
    /** mqtt client connect时间, 校验通过后的时间 */
    private long connectTime;
    /** mqtt client disConnect时间 */
    private long disConnectTime;
    /** 会话过期间隔(秒), 0xFFFFFFFF即为永不过期 */
    private int expiryInternal;
    /**
     * 会话过期时间(毫秒), 0xFFFFFFFF即为永不过期
     * 只有在disconnect消息和session close时更新
     */
    private long expireTime;
    /** mqtt client user name */
    private String userName;
    /** mqtt session status */
    private volatile SessionStatus status = SessionStatus.INIT;
    /** 待发送的qos>0 mqtt message */
    private InflightWindow inflightWindow;
    /** 遗愿 */
    private Will will;
    /** 用于控制建立connection后, client还不发送connect消息, 则broker主动关闭connection */
    private Disposable deferCloseWithoutConnMsgDisposable;
    /** mqtt client 订阅 */
    private Set<TopicSubscription> subscriptions;
    /** 发送mqtt消息id */
    private AtomicInteger messageIdGenerator;
    /**
     * 接到来自于该mqtt session的exactly once消息缓存
     *
     * @see io.netty.handler.codec.mqtt.MqttQoS#EXACTLY_ONCE
     */
    private Map<Integer, MqttQos2PubMessage> qos2MessageCache;
    /** key -> topic别名alias, value -> 真实topic */
    private NonBlockingHashMap<Integer, String> alias2TopicName;
    /** 单个连接消息速率整型 */
    private RateLimiter messageRateLimiter;

    public MqttSession(MqttBrokerContext brokerContext, Connection connection) {
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
        MqttMessageType mqttMessageType = mqttMessage.fixedHeader().messageType();
        log.debug("session {} send {} message", getConnection(), mqttMessageType);


        if (MqttMessageType.PUBLISH.equals(mqttMessageType) &&
                mqttMessage.fixedHeader().qosLevel().value() > 0 &&
                tryEnqueueInflightQueue(MqttMessageContext.common((MqttPublishMessage) mqttMessage, brokerContext.getBrokerId(), clientId))) {
            //仅针对qos1和qos2的publish message
            //进入了inflight queue等待
            return Mono.empty();
        }

        if (retry) {
            //待发送的mqtt消息
            MqttMessage reply = getReplyMqttMessage(mqttMessage);

            Runnable retryTask = () -> sendMessage0(Mono.just(reply)).subscribe();
            Runnable cleaner = () -> {
                //完全释放
                MqttPublishMessage pubReply = (MqttPublishMessage) reply;
                for (int i = 0; i < pubReply.refCnt(); i++) {
                    ReactorNetty.safeRelease(reply);
                }
            };

            RetryService retryService = brokerContext.getRetryService();
            //开启retry task, 最大重试次数为5, 间隔3s
            long uuid = genMqttMessageRetryId(mqttMessageType, MqttMessageHelper.getMessageId(mqttMessage));
            retryService.execRetry(new PublishRetry(uuid, retryTask, cleaner, retryService));

            return sendMessage0(Mono.just(mqttMessage));
        } else {
            return sendMessage0(Mono.just(mqttMessage));
        }
    }

    /**
     * Inflight queue入队
     *
     * @return true表示入队成功, 则等待有空闲token再处理publish消息; 否则马上处理
     * @see org.kin.mqtt.broker.core.message.handler.PublishHandler
     */
    public boolean tryEnqueueInflightQueue(MqttMessageContext<MqttPublishMessage> messageContext) {
        //client的pub消息
        return !inflightWindow.takeQuota(messageContext);
    }

    /**
     * 接收到broker -> client的publish的响应消息, 即PUBACK或PUBCOMP
     */
    public void onRecPubRespMessage() {
        MqttMessage mqttMessage = inflightWindow.returnQuota();
        if (Objects.nonNull(mqttMessage)) {
            sendMessage(mqttMessage, mqttMessage.fixedHeader().qosLevel().value() > 0).subscribe();
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
    private Mono<Void> sendMessage0(Mono<MqttMessage> messageMono) {
        if (isChannelActive() && isChannelWritable()) {
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
     * @return retry唯一ID
     */
    public long genMqttMessageRetryId(MqttMessageType type, Integer messageId) {
        return RetryService.genMqttMessageRetryId(this, type, messageId);
    }

    /**
     * @return 下一消息id
     */
    public int nextMessageId() {
        int value;
        while (qos2MessageCache.containsKey(value = messageIdGenerator.incrementAndGet())) {
            if (value >= MqttMessageHelper.MAX_MESSAGE_ID) {
                //消息id有最大限制
                synchronized (this) {
                    value = messageIdGenerator.incrementAndGet();
                    if (value >= MqttMessageHelper.MAX_MESSAGE_ID) {
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
     * @param messageContext mqtt publish message context
     * @return complete signal
     */
    public Mono<Void> cacheQos2Message(int messageId, MqttMessageContext<MqttPublishMessage> messageContext) {
        return Mono.fromRunnable(() -> {
            long expireTimeMs = messageContext.getExpireTime();
            Timeout expireTimeout = null;
            if (expireTimeMs > 0) {
                HashedWheelTimer bsTimer = brokerContext.getBsTimer();
                expireTimeout = bsTimer.newTimeout(t -> removeQos2Message(messageId), expireTimeMs, TimeUnit.MILLISECONDS);
            }
            qos2MessageCache.put(messageId, new MqttQos2PubMessage(messageContext, expireTimeout));
        });
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
    @Nullable
    public MqttMessageContext<MqttPublishMessage> removeQos2Message(int messageId) {
        MqttQos2PubMessage qos2PubMessage = qos2MessageCache.remove(messageId);
        if (Objects.nonNull(qos2PubMessage)) {
            qos2PubMessage.cancelExpireTimeout();
            return qos2PubMessage.getMessageContext();
        }
        return null;
    }

    /**
     * @return mqtt session是否在线
     */
    public boolean isOnline() {
        return status == SessionStatus.ONLINE;
    }

    /**
     * @return mqtt session是否离线
     */
    public boolean isOffline() {
        return status == SessionStatus.OFFLINE;
    }

    /**
     * 用于控制建立connection后, client还不发送connect消息, 则broker主动关闭connection
     *
     * @return this
     */
    public MqttSession deferCloseWithoutConnMsg() {
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
     * 接受connect消息并成功通过校验后, 执行session初始化
     */
    public MqttSession onConnect(String clientId,
                                 MqttConnectVariableHeader variableHeader,
                                 MqttConnectPayload payload) {
        return onConnect(clientId, variableHeader, payload, null);
    }

    /**
     * 接受connect消息并成功通过校验后, 执行session初始化
     *
     * @param replica 不为null时, 表示有持久化mqtt session
     */
    @SuppressWarnings("unchecked")
    public MqttSession onConnect(String clientId,
                                 MqttConnectVariableHeader variableHeader,
                                 MqttConnectPayload payload,
                                 @Nullable MqttSessionReplica replica) {
        //关闭延迟关闭没有发起connect的mqtt client
        if (deferCloseWithoutConnMsgDisposable != null && !deferCloseWithoutConnMsgDisposable.isDisposed()) {
            deferCloseWithoutConnMsgDisposable.dispose();
            //release, 后面没用了释放对象
            deferCloseWithoutConnMsgDisposable = null;
        }

        //初始化字段, 因为离线后, 新连接会创建mqtt session, 选择不在定义时初始化字段,
        //则是在session持久化场景下可以减少新对象分配(新mqtt session对象仅用于恢复旧mqtt session, 然后会被抛弃)
        //此时不为null
        this.host = connection.address().toString().split(":")[0];
        this.channelHashCode = connection.channel().hashCode();
        this.clientId = clientId;
        firstConnectTime = System.currentTimeMillis();
        connectTime = System.currentTimeMillis();
        status = SessionStatus.ONLINE;
        userName = payload.userName();
        MqttProperties properties = variableHeader.properties();
        if (!variableHeader.isCleanSession()) {
            //设置为永不过期
            expiryInternal = 0xFFFFFFFF;
            MqttProperties.MqttProperty<Integer> sessionExpiryIntervalProp = properties.getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
            if (Objects.nonNull(sessionExpiryIntervalProp)) {
                expiryInternal = sessionExpiryIntervalProp.value();
            }
        }
        int receiveMaximumPropVal = 0;
        MqttProperties.MqttProperty<Integer> receiveMaximumProp = properties.getProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value());
        if (Objects.nonNull(receiveMaximumProp)) {
            receiveMaximumPropVal = receiveMaximumProp.value();
        }

        inflightWindow = new InflightWindow(receiveMaximumPropVal);
        subscriptions = new NonBlockingHashSet<>();
        messageIdGenerator = new AtomicInteger();
        qos2MessageCache = new NonBlockingHashMap<>();
        alias2TopicName = new NonBlockingHashMap<>();
        int connMessagePerSec = brokerContext.getBrokerConfig().getConnMessagePerSec();
        if (connMessagePerSec > 0) {
            messageRateLimiter = RateLimiter.create(connMessagePerSec);
        }

        //从持久化session数据中恢复session状态
        recoverFromReplica(replica);

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
            MqttProperties.MqttProperty<Integer> pubExpiryIntervalProp = properties.getProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value());
            long expiryInterval = -1;
            if (Objects.nonNull(pubExpiryIntervalProp)) {
                //已过期
                expiryInterval = TimeUnit.SECONDS.toMillis(pubExpiryIntervalProp.value());
            }
            will = Will.builder()
                    .setRetain(variableHeader.isWillRetain())
                    .topic(payload.willTopic())
                    .message(payload.willMessageInBytes())
                    .qos(MqttQoS.valueOf(variableHeader.willQos()))
                    .delay(willDelay)
                    .expiryInterval(expiryInterval)
                    .build();
            //注册dispose后will逻辑
            afterDispose(this::handleWillAfterClose);
        }

        //注册dispose逻辑
        afterDispose(this::close0);

        //register session
        if (brokerContext.getSessionManager().register(clientId, this)) {
            brokerContext.broadcastEvent(new MqttClientRegisterEvent(this));
            brokerContext.broadcastEvent(new OnlineClientNumEvent(brokerContext.getSessionManager().size()));
        }

        return this;
    }

    /**
     * 从持久化session数据中恢复session状态
     */
    private void recoverFromReplica(@Nullable MqttSessionReplica replica) {
        if (Objects.isNull(replica)) {
            return;
        }

        this.firstConnectTime = replica.getFirstConnectTime();
        this.messageIdGenerator.set(replica.getMessageId());
        if (CollectionUtils.isNonEmpty(replica.getSubscriptions())) {
            //恢复订阅关系
            List<TopicSubscription> subscriptions = replica.getSubscriptions()
                    .stream()
                    .map(ts -> new TopicSubscription(ts, this))
                    .collect(Collectors.toList());
            brokerContext.getTopicManager().addSubscriptions(subscriptions);

            brokerContext.broadcastEvent(new MqttSubscribeEvent(this, subscriptions));
        }
    }

    /**
     * connection close主要逻辑
     */
    private void close0() {
        if (isOffline()) {
            return;
        }

        log.info("mqtt session closed, {}", this);

        //更新状态
        offline();
        onDisconnect(expiryInternal);

        //持久化session
        tryPersist();

        //先获取订阅消息
        List<String> unsubscribeTopics = subscriptions.stream().map(TopicSubscription::getTopic).collect(Collectors.toList());

        //取消session注册
        brokerContext.getSessionManager().remove(clientId);
        //取消订阅
        //!!会清空MqttSession.subscriptions
        brokerContext.getTopicManager().removeAllSubscriptions(this);

        //将inflight message保存为offline message, 待client重新上线后重试
        MqttMessageStore messageStore = brokerContext.getMessageStore();
        List<MqttMessageReplica> inflightMessageReplicas = inflightWindow.drainInflightMqttMessages()
                .stream()
                .map(MqttMessageContext::toReplica)
                .collect(Collectors.toList());
        messageStore.saveOfflineMessages(clientId, inflightMessageReplicas);

        brokerContext.broadcastEvent(new MqttClientUnregisterEvent(this));
        brokerContext.broadcastEvent(new OnlineClientNumEvent(brokerContext.getSessionManager().size()));
        brokerContext.broadcastEvent(new MqttClientDisConnEvent(this));
        brokerContext.broadcastEvent(new MqttUnsubscribeEvent(this, unsubscribeTopics));
    }

    /**
     * mqtt session close
     *
     * @return close complete signal
     */
    public Mono<Void> close() {
        return Mono.fromRunnable(() -> {
            if (isOffline()) {
                return;
            }

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
        if (isExpire()) {
            //会话过期, 马上发送will
            delay = 0;
        }

        WillDelayTask willTask = new WillDelayTask(brokerContext, clientId, connection.channel().alloc(), will);
        if (delay > 0) {
            //will延迟
            HashedWheelTimer bsTimer = brokerContext.getBsTimer();
            bsTimer.newTimeout(willTask, delay, TimeUnit.SECONDS);
        } else {
            //无延迟
            willTask.runNow();
        }
    }

    /**
     * 将状态设置为{@link SessionStatus#OFFLINE}
     */
    private void offline() {
        status = SessionStatus.OFFLINE;
    }

    /**
     * 更新session过期时间, 目前仅在处理disconnect消息和session close时调用
     * 二选一, 只能由其中一个操作触发
     *
     * @param sessionExpiryInterval 会话过期间隔(秒)
     */
    public void updateExpireTime(int sessionExpiryInterval) {
        if (this.expireTime != 0) {
            return;
        }

        if (sessionExpiryInterval < 0) {
            this.expireTime = 0xFFFFFFFFL;
        } else if (sessionExpiryInterval > 0) {
            this.expireTime = disConnectTime + TimeUnit.SECONDS.toMillis(sessionExpiryInterval);
        }
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
     * 过滤已注册的topic订阅, 同时使用新订阅qos替换旧订阅qos
     */
    public Set<TopicSubscription> filterRegisteredTopicSubscriptions(Set<TopicSubscription> subscriptions) {
        Map<String, TopicSubscription> topic2qos = subscriptions.stream().collect(Collectors.toMap(TopicSubscription::getTopic, ts -> ts));
        for (TopicSubscription subscription : this.subscriptions) {
            TopicSubscription newSubscription = topic2qos.remove(subscription.getRawTopic());
            if (Objects.nonNull(newSubscription)) {
                subscription.setQos(newSubscription.getQos());
            }
        }

        return new HashSet<>(topic2qos.values());
    }

    /**
     * 检查单个连接消息速率整型
     * 不精准, 这里是处理publish消息时做检查, 那么还存在可能部分消息解析好但等待处理
     */
    public void checkPubMessageRate() {
        if (Objects.isNull(messageRateLimiter) || messageRateLimiter.tryAcquire()) {
            return;
        }

        //没有拿到令牌
        long now = System.nanoTime();
        long nextSec = TimeUnit.SECONDS.toNanos(TimeUnit.NANOSECONDS.toSeconds(now) + 1);
        //20ms兜底
        long waitTime = nextSec - now + TimeUnit.MILLISECONDS.toNanos(20);
        if (waitTime > 0) {
            log.warn("mqtt client({}) send publish message too fast, reach limit {} msg/s",
                    clientId, brokerContext.getBrokerConfig().getConnMessagePerSec());
            Channel channel = connection.channel();
            channel.config().setAutoRead(false);
            channel.eventLoop().schedule(() -> {
                channel.config().setAutoRead(true);
                log.warn("mqtt broker available to read mqtt client({})'s  publish message", clientId);
            }, waitTime, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * @return 底层channel是否active
     */
    public boolean isChannelActive() {
        return Objects.nonNull(connection) && connection.channel().isActive();
    }

    /**
     * @return 底层channel是否允许write bytes
     */
    public boolean isChannelWritable() {
        return Objects.nonNull(connection) && connection.channel().isWritable();
    }

    /**
     * 将{@link MqttSession}转换成{@link MqttSessionReplica}
     */
    public MqttSessionReplica toReplica() {
        MqttSessionReplica replica = new MqttSessionReplica();
        replica.setBrokerId(brokerContext.getBrokerId());
        replica.setClientId(clientId);
        replica.setFirstConnectTime(firstConnectTime);
        replica.setConnectTime(connectTime);
        replica.setDisConnectTime(disConnectTime);
        replica.setExpireTime(expireTime);
        Set<TopicSubscriptionReplica> subscriptionReplicas = new HashSet<>(subscriptions.size());
        for (TopicSubscription subscription : subscriptions) {
            subscriptionReplicas.add(subscription.toReplica());
        }
        replica.setSubscriptions(subscriptionReplicas);
        replica.setMessageId(messageIdGenerator.get());
        return replica;
    }

    /**
     * 尝试持久化mqtt session
     * 只有当mqtt session=-1或者>0才会持久化
     */
    public void tryPersist() {
        if (isCleanAfterOffline()) {
            return;
        }

        //持久化
        brokerContext.getSessionStore()
                .saveAsync(toReplica());
    }

    /**
     * 判断session是否过期
     *
     * @return session是否过期
     */
    public boolean isExpire() {
        return isOffline() && expireTime > 0 && System.currentTimeMillis() >= expireTime;
    }

    /**
     * 首次connect time != 本次connect time, 即该session是新建的
     */
    public boolean isSessionPresent() {
        return firstConnectTime != connectTime;
    }

    /**
     * disconnect处理
     *
     * @param sessionExpiryInterval 会话过期间隔(秒)
     */
    public void onDisconnect(int sessionExpiryInterval) {
        disConnectTime = System.currentTimeMillis();
        updateExpireTime(sessionExpiryInterval);
    }

    /**
     * session是否持久化
     *
     * @return session是否持久化, true表示非持久化
     */
    public boolean isCleanAfterOffline() {
        return expiryInternal == 0;
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

    public long getFirstConnectTime() {
        return firstConnectTime;
    }

    public long getConnectTime() {
        return connectTime;
    }

    public String getUserName() {
        return userName;
    }

    public long getDisConnectTime() {
        return disConnectTime;
    }

    public int getChannelHashCode() {
        return channelHashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MqttSession)) {
            return false;
        }
        MqttSession that = (MqttSession) o;
        return Objects.equals(connection, that.connection) && Objects.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connection, clientId);
    }

    @Override
    public String toString() {
        return "MqttSession{" +
                "connection=" + connection +
                ", channelHashCode=" + channelHashCode +
                ", host='" + host + '\'' +
                ", clientId='" + clientId + '\'' +
                ", firstConnectTime=" + firstConnectTime +
                ", connectTime=" + connectTime +
                ", disConnectTime=" + disConnectTime +
                ", expiryInternal=" + expiryInternal +
                ", expireTime=" + expireTime +
                ", userName='" + userName + '\'' +
                ", status=" + status +
                '}';
    }
}
