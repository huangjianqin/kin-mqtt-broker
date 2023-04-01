package org.kin.mqtt.broker.core;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.kin.framework.event.EventListener;
import org.kin.framework.reactor.event.EventConsumer;
import org.kin.framework.utils.SysUtils;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.acl.NoneAclService;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.auth.NoneAuthService;
import org.kin.mqtt.broker.bridge.Bridge;
import org.kin.mqtt.broker.cluster.BrokerManager;
import org.kin.mqtt.broker.cluster.StandaloneBrokerManager;
import org.kin.mqtt.broker.core.handler.ByteBuf2WsFrameEncoder;
import org.kin.mqtt.broker.core.handler.MqttBrokerHandler;
import org.kin.mqtt.broker.core.handler.WsFrame2ByteBufDecoder;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.topic.share.RandomShareSubLoadBalance;
import org.kin.mqtt.broker.core.topic.share.ShareSubLoadBalance;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.kin.mqtt.broker.store.MemoryMessageStore;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.mqtt.broker.systopic.TotalClientNumPublisher;
import org.kin.transport.netty.ServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

import java.util.*;

/**
 * mqtt broker启动类
 *
 * @author huangjianqin
 * @date 2022/11/6
 */
public class MqttBrokerBootstrap extends ServerTransport<MqttBrokerBootstrap> {
    private static final Logger log = LoggerFactory.getLogger(MqttBrokerBootstrap.class);

    private final MqttBrokerConfig config;
    /** 注册的interceptor */
    private final List<Interceptor> interceptors = new LinkedList<>();
    /** auth service, 默认不进行校验 */
    private AuthService authService = NoneAuthService.INSTANCE;
    /** mqtt broker集群管理. 默认单节点模式 */
    private BrokerManager brokerManager = StandaloneBrokerManager.INSTANCE;
    /** mqtt消息外部存储, 默认存储在jvm内存 */
    private MqttMessageStore messageStore = new MemoryMessageStore();
    /** 规则链定义 */
    private List<RuleDefinition> ruleDefinitions = new LinkedList<>();
    /** 数据桥接实现 */
    private final List<Bridge> bridges = new LinkedList();
    /** 访问控制权限管理 */
    private AclService aclService = NoneAclService.INSTANCE;
    /** 事件consumer */
    private final List<Object> eventConsumers = new LinkedList<>();
    /** 共享订阅负载均衡实现 */
    private ShareSubLoadBalance shareSubLoadBalance = RandomShareSubLoadBalance.INSTANCE;

    public static MqttBrokerBootstrap create() {
        return new MqttBrokerBootstrap(MqttBrokerConfig.create());
    }

    public static MqttBrokerBootstrap create(MqttBrokerConfig config) {
        return new MqttBrokerBootstrap(config);
    }

    private MqttBrokerBootstrap(MqttBrokerConfig config) {
        this.config = config;

        //设置ssl相关
        ssl(config.isSsl());
        String caFile = config.getCaFile();
        if (Objects.nonNull(caFile)) {
            caFile(caFile);
        }

        String certFile = config.getCertFile();
        if (Objects.nonNull(certFile)) {
            certFile(certFile);
        }

        String certKeyFile = config.getCertKeyFile();
        if (Objects.nonNull(certKeyFile)) {
            certKeyFile(certKeyFile);
        }
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
    public MqttBrokerBootstrap rule(RuleDefinition definition) {
        this.ruleDefinitions.add(definition);
        return this;
    }

    /**
     * 数据桥接定义
     */
    public MqttBrokerBootstrap bridge(Bridge bridge) {
        bridges.add(bridge);
        return this;
    }

    /**
     * 数据桥接定义
     */
    public MqttBrokerBootstrap bridges(List<Bridge> bridges) {
        bridges.forEach(this::bridge);
        return this;
    }

    /**
     * 访问控制权限管理
     */
    public MqttBrokerBootstrap aclService(AclService aclService) {
        this.aclService = aclService;
        return this;
    }

    /**
     * 事件consumer
     */
    public MqttBrokerBootstrap eventConsumers(EventConsumer<?>... consumers) {
        this.eventConsumers.addAll(Arrays.asList(consumers));
        return this;
    }

    /**
     * 事件consumer
     * 1. {@link EventConsumer}实现类
     * 2. 带{@link org.kin.framework.event.EventFunction}的实例
     */
    public MqttBrokerBootstrap eventConsumers(Collection<EventConsumer<?>> consumers) {
        this.eventConsumers.addAll(consumers);
        return this;
    }

    /**
     * 事件consumer
     * 带{@link org.kin.framework.event.EventFunction}的实例
     */
    public MqttBrokerBootstrap eventConsumers(Object... consumers) {
        for (Object consumer : consumers) {
            if (!consumer.getClass().isAnnotationPresent(EventListener.class)) {
                throw new IllegalArgumentException(String.format("%s must be annotated with @%s", consumer.getClass(), EventListener.class.getSimpleName()));
            }
        }
        this.eventConsumers.addAll(Arrays.asList(consumers));
        return this;
    }

    /**
     * 配置共享订阅负载均衡实现, 默认随机
     *
     * @param shareSubLoadBalance 共享订阅负载均衡实现
     */
    public MqttBrokerBootstrap shareSubLoadBalance(ShareSubLoadBalance shareSubLoadBalance) {
        this.shareSubLoadBalance = shareSubLoadBalance;
        return this;
    }

    /**
     * start mqtt server及其admin server
     */
    public MqttBroker start() {
        config.selfCheck();
        checkRequire();

        //系统topic配置
        if (config.isEnableSysTopic()) {
            configSysTopic();
        }

        int port = config.getPort();
        MqttBrokerContext brokerContext = new MqttBrokerContext(config, new MqttMessageDispatcher(interceptors),
                authService, brokerManager, messageStore,
                ruleDefinitions,
                aclService);

        //启动mqtt broker
        LoopResources loopResources = LoopResources.create("kin-mqtt-server-" + port, 2, SysUtils.DOUBLE_CPU, false);
        List<Mono<DisposableServer>> disposableServerMonoList = new LinkedList<>();
        //tcp
        TcpServer tcpServer = TcpServer.create();
        if (isSsl()) {
            tcpServer = tcpServer.secure(this::secure);
        }
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
                    int connBytesPerSec = config.getConnBytesPerSec();
                    if (connBytesPerSec > 0) {
                        //流量整形
                        connection.addHandlerLast(new ChannelTrafficShapingHandler(0, connBytesPerSec));
                    }

                    connection
                            //mqtt decoder encoder
                            .addHandlerLast(new MqttDecoder(config.getMessageMaxSize()))
                            .addHandlerLast(MqttEncoder.INSTANCE)
                            .addHandlerLast(MqttBrokerHandler.DEFAULT);
                    onMqttClientConnected(brokerContext, new MqttChannel(brokerContext, connection));
                });

        applyOptions(tcpServer);
        applyChildOptions(tcpServer);

        Mono<DisposableServer> disposableServerMono = tcpServer.bind()
                .doOnNext(d -> {
                    //定义mqtt broker close逻辑
                    d.onDispose(() -> log.info("mqtt broker(port:{}) closed", port));
                })
                .doOnSuccess(d -> log.info("mqtt broker started on port({})", port))
                .cast(DisposableServer.class);
        disposableServerMonoList.add(disposableServerMono);

        //websocket
        int wsPort = config.getWsPort();
        if (wsPort > 0) {
            TcpServer wsServer = TcpServer.create();
            if (isSsl()) {
                wsServer = wsServer.secure(this::secure);
            }
            wsServer.port(wsPort)
                    //打印底层event和二进制内容
//                .wiretap(false)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .metrics(true)
                    .runOn(loopResources)
                    .doOnConnection(connection -> {
                        int connBytesPerSec = config.getConnBytesPerSec();
                        if (connBytesPerSec > 0) {
                            //流量整形
                            connection.addHandlerLast(new ChannelTrafficShapingHandler(0, connBytesPerSec));
                        }

                        connection
                                //websocket相关
                                .addHandlerLast(new HttpServerCodec())
                                .addHandlerLast(new HttpObjectAggregator(65536))
                                .addHandlerLast(new WebSocketServerProtocolHandler(config.getWsPath(), "mqtt, mqttv3.1, mqttv3.1.1"))
                                .addHandlerLast(new WsFrame2ByteBufDecoder())
                                .addHandlerLast(new ByteBuf2WsFrameEncoder())
                                //mqtt decoder encoder
                                .addHandlerLast(new MqttDecoder(config.getMessageMaxSize()))
                                .addHandlerLast(MqttEncoder.INSTANCE);
                        onMqttClientConnected(brokerContext, new MqttChannel(brokerContext, connection));
                    });

            applyOptions(wsServer);
            applyChildOptions(wsServer);

            disposableServerMono = wsServer.bind()
                    .doOnNext(d -> {
                        //定义mqtt broker over websocket close逻辑
                        d.onDispose(() -> log.info("mqtt broker over websocket(port:{}) closed", wsPort));
                    })
                    .doOnSuccess(d -> log.info("mqtt broker over websocket started on port({})", wsPort))
                    .cast(DisposableServer.class);
            disposableServerMonoList.add(disposableServerMono);
        }

        //集群初始化
        initBrokerManager(brokerContext);
        //init bridge manager
        addBridges(brokerContext);

        return new MqttBroker(brokerContext, disposableServerMonoList, () -> {
            loopResources.dispose();
            brokerContext.close();
        });
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
                .publishOn(brokerContext.getMqttBizScheduler())
                //mqtt消息处理
                .subscribe(mqttMessage -> brokerContext.getDispatcher().dispatch(MqttMessageWrapper.common(mqttMessage), mqttChannel, brokerContext));
    }

    /**
     * {@link BrokerManager}初始化完成之后的操作
     */
    private void initBrokerManager(MqttBrokerContext brokerContext) {
        brokerContext.getBrokerManager().start(brokerContext)
                .then(Mono.fromRunnable(() -> brokerManager.clusterMqttMessages()
                        .onErrorResume(e -> Mono.empty())
                        .publishOn(brokerContext.getMqttBizScheduler())
                        .subscribe(clusterMessage -> brokerContext.getDispatcher().dispatch(
                                        MqttMessageWrapper.fromCluster(clusterMessage),
                                        new VirtualMqttChannel(brokerContext, clusterMessage.getClientId()),
                                        brokerContext),
                                t -> log.error("broker manager handle cluster message error", t))))
                .subscribe();
    }

    /**
     * 系统topic配置
     */
    private void configSysTopic() {
        eventConsumers(new TotalClientNumPublisher());
    }

    /**
     * 注册{@link  Bridge}实现
     */
    private void addBridges(MqttBrokerContext brokerContext) {
        bridges.forEach(b -> brokerContext.getBridgeManager().addBridge(b));
    }

    //getter
    public MqttBrokerConfig getConfig() {
        return config;
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

    public List<RuleDefinition> getRuleDefinitions() {
        return ruleDefinitions;
    }

    public AclService getAclService() {
        return aclService;
    }
}
