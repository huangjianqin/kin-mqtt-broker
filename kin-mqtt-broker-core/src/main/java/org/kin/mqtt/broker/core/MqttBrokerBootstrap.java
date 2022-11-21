package org.kin.mqtt.broker.core;

import com.google.common.base.Preconditions;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.kin.framework.utils.SysUtils;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.auth.NoneAuthService;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.cluster.StandaloneBrokerManager;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.rule.RuleChainDefinition;
import org.kin.mqtt.broker.store.MemoryMessageStore;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.transport.netty.ServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

import java.util.LinkedList;
import java.util.List;

/**
 * mqtt broker启动类
 *
 * @author huangjianqin
 * @date 2022/11/6
 */
public final class MqttBrokerBootstrap extends ServerTransport {
    private static final Logger log = LoggerFactory.getLogger(MqttBrokerBootstrap.class);

    /** mqtt broker port, default 1883 */
    private int port = 1883;
    /** 最大消息大小, 默认4MB */
    private int messageMaxSize = 4194304;
    /** 注册的interceptor */
    private final List<Interceptor> interceptors = new LinkedList<>();
    /** mqtt broker集群管理. 默认单节点模式 */
    private BrokerManager brokerManager = StandaloneBrokerManager.INSTANCE;
    /** mqtt消息外部存储, 默认存储在jvm内存 */
    private MqttMessageStore messageStore = new MemoryMessageStore();
    /** auth service, 默认不进行校验 */
    private AuthService authService = NoneAuthService.INSTANCE;
    /** 规则链定义 */
    private List<RuleChainDefinition> ruleChainDefinitions = new LinkedList<>();

    public static MqttBrokerBootstrap create() {
        return new MqttBrokerBootstrap();
    }

    private MqttBrokerBootstrap() {
    }

    /**
     * 定义mqtt server port
     */
    public MqttBrokerBootstrap port(int port) {
        Preconditions.checkArgument(port > 0, "port must be greater than 0");
        this.port = port;
        return this;
    }

    /**
     * 最大消息大小设置
     */
    public MqttBrokerBootstrap messageMaxSize(int messageMaxSize) {
        Preconditions.checkArgument(port > 0, "messageMaxSize must be greater than 0");
        this.messageMaxSize = messageMaxSize;
        return this;
    }

    /**
     * 注册{@link Interceptor}
     */
    public MqttBrokerBootstrap interceptor(Interceptor interceptor) {
        interceptors.add(interceptor);
        return this;
    }

    /**
     * 注册{@link Interceptor}
     */
    public MqttBrokerBootstrap interceptors(List<Interceptor> interceptors) {
        interceptors.addAll(interceptors);
        return this;
    }

    /**
     * mqtt broker集群管理
     */
    public MqttBrokerBootstrap brokerManager(BrokerManager brokerManager) {
        this.brokerManager = brokerManager;
        return this;
    }

    /**
     * mqtt消息外部存储
     */
    public MqttBrokerBootstrap messageStore(MqttMessageStore messageStore) {
        this.messageStore = messageStore;
        return this;
    }

    /**
     * auth service
     */
    public MqttBrokerBootstrap authService(AuthService authService) {
        this.authService = authService;
        return this;
    }

    /**
     * 规则链配置
     */
    public MqttBrokerBootstrap ruleChain(RuleChainDefinition definition) {
        this.ruleChainDefinitions.add(definition);
        return this;
    }

    /**
     * start mqtt server及其admin server
     */
    public MqttBroker start() {
        TcpServer tcpServer = TcpServer.create();
        if (isSsl()) {
            tcpServer = tcpServer.secure(this::secure);
        }

        MqttBrokerContext brokerContext = new MqttBrokerContext(port, new MqttMessageDispatcher(interceptors),
                authService, brokerManager, messageStore, ruleChainDefinitions);
        BrokerManager brokerManager;

        //启动mqtt broker
        LoopResources loopResources = LoopResources.create("kin-mqtt-server-" + port, 2, SysUtils.DOUBLE_CPU, false);
        tcpServer = tcpServer.port(port)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                //打印底层event和二进制内容
//                .wiretap(false)
                .metrics(true)
                .runOn(loopResources)
                .doOnConnection(connection -> {
                    connection.addHandlerFirst(new MqttDecoder(messageMaxSize))
                            .addHandlerFirst(MqttEncoder.INSTANCE);
                    onMqttClientConnected(brokerContext, new MqttChannel(brokerContext, connection));
                });

        //自定义mqtt server配置
        tcpServer = customServerTransport(tcpServer);
        Mono<DisposableServer> disposableServerMono = tcpServer.bind()
                .doOnNext(d -> {
                    //定义mqtt broker close逻辑
                    d.onDispose(loopResources);
                    d.onDispose(brokerContext::close);
                    d.onDispose(() -> log.info("mqtt broker(port:{}) closed", port));
                })
                .doOnSuccess(d -> log.info("mqtt server started on port({})", port))
                .cast(DisposableServer.class);

        //集群初始化
        initBrokerManager(brokerContext);

        return new MqttBroker(brokerContext, disposableServerMono);
    }

    /**
     * mqtt client建立连接时触发, mqtt channel配置以及处理mqtt消息逻辑
     */
    private void onMqttClientConnected(MqttBrokerContext brokerContext, MqttChannel mqttChannel) {
        mqttChannel.deferCloseWithoutConnMsg()
                .getConnection()
                .inbound()
                //处理inbound bytes
                .receiveObject()
                .cast(MqttMessage.class)
                .onErrorContinue((throwable, o) -> {
                    log.error("mqtt message receive error {}", o, throwable);
                })
                //过滤解包失败的
                .filter(mqttMessage -> mqttMessage.decoderResult().isSuccess())
                .doOnNext(mqttMessage -> {
                    //此publish complete会释放reference count, 所以先retain. 就像是SimpleChannelInboundHandler
                    if (mqttMessage instanceof MqttPublishMessage) {
                        MqttPublishMessage publishMessage = (MqttPublishMessage) mqttMessage;
                        publishMessage.retain();
                    }
                })
                .publishOn(brokerContext.getMqttMessageHandleScheduler())
                //mqtt消息处理
                .subscribe(mqttMessage -> brokerContext.getDispatcher().dispatch(MqttMessageWrapper.common(mqttMessage), mqttChannel, brokerContext));
    }

    /**
     * {@link BrokerManager}初始化完成之后的操作
     */
    private void initBrokerManager(MqttBrokerContext brokerContext) {
        brokerContext.getBrokerManager().start()
                .then(Mono.fromRunnable(() -> brokerManager.clusterMqttMessages()
                        .onErrorResume(e -> Mono.empty())
                        .publishOn(brokerContext.getMqttMessageHandleScheduler())
                        .subscribe(clusterMessage -> brokerContext.getDispatcher().dispatch(
                                        MqttMessageWrapper.fromCluster(clusterMessage),
                                        new FakeMqttChannel(brokerContext, clusterMessage.getClientId()),
                                        brokerContext),
                                t -> log.error("broker manager handle cluster message error", t))))
                .subscribe();
    }

    //getter
    public int getPort() {
        return port;
    }

    public int getMessageMaxSize() {
        return messageMaxSize;
    }

    public List<Interceptor> getInterceptors() {
        return interceptors;
    }

    public MqttMessageStore getMessageStore() {
        return messageStore;
    }

    public AuthService getAuthService() {
        return authService;
    }

    public BrokerManager getBrokerManager() {
        return brokerManager;
    }

    public List<RuleChainDefinition> getRuleChainDefinitions() {
        return ruleChainDefinitions;
    }
}
