package org.kin.mqtt.broker.core;

import com.google.common.base.Preconditions;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.kin.framework.reactor.event.EventConsumer;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.framework.utils.StringUtils;
import org.kin.framework.utils.SysUtils;
import org.kin.mqtt.broker.ServerCustomizer;
import org.kin.mqtt.broker.acl.AclService;
import org.kin.mqtt.broker.acl.NoneAclService;
import org.kin.mqtt.broker.auth.AuthService;
import org.kin.mqtt.broker.auth.NoneAuthService;
import org.kin.mqtt.broker.bridge.BridgeConfiguration;
import org.kin.mqtt.broker.core.event.MqttEventConsumer;
import org.kin.mqtt.broker.core.handler.ByteBuf2WsFrameEncoder;
import org.kin.mqtt.broker.core.handler.MqttBrokerHandler;
import org.kin.mqtt.broker.core.handler.WsFrame2ByteBufDecoder;
import org.kin.mqtt.broker.core.message.MqttMessageContext;
import org.kin.mqtt.broker.core.topic.share.RandomShareSubLoadBalance;
import org.kin.mqtt.broker.core.topic.share.ShareSubLoadBalance;
import org.kin.mqtt.broker.rule.RuleDefinition;
import org.kin.mqtt.broker.store.DefaultMqttMessageStore;
import org.kin.mqtt.broker.store.MqttMessageStore;
import org.kin.mqtt.broker.systopic.impl.OnlineClientNumPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpServer;

import javax.net.ssl.SSLException;
import java.io.File;
import java.security.cert.CertificateException;
import java.util.*;

/**
 * mqtt broker启动类
 *
 * @author huangjianqin
 * @date 2022/11/6
 */
public class MqttBrokerBootstrap {
    private static final Logger log = LoggerFactory.getLogger(MqttBrokerBootstrap.class);
    private static final String[] PROTOCOLS = new String[]{"TLSv1.3", "TLSv.1.2"};

    private final MqttBrokerConfig config;
    private ServerCustomizer serverCustomizer;
    /** 注册的interceptor */
    private final List<Interceptor> interceptors = new LinkedList<>();
    /** auth service, 默认不进行校验 */
    private AuthService authService = NoneAuthService.INSTANCE;
    /** mqtt消息外部存储, 默认存储在jvm内存 */
    private MqttMessageStore messageStore = new DefaultMqttMessageStore();
    /** 规则链定义 */
    private final List<RuleDefinition> ruleDefinitions = new LinkedList<>();
    /** 访问控制权限管理 */
    private AclService aclService = NoneAclService.INSTANCE;
    /** 事件consumer */
    @SuppressWarnings("rawtypes")
    private final List<MqttEventConsumer> eventConsumers = new LinkedList<>();
    /** 共享订阅负载均衡实现 */
    private ShareSubLoadBalance shareSubLoadBalance = RandomShareSubLoadBalance.INSTANCE;
    /** 桥接定义 */
    private final List<BridgeConfiguration> bridgeConfigurations = new LinkedList<>();

    public static MqttBrokerBootstrap create() {
        return new MqttBrokerBootstrap(MqttBrokerConfig.create());
    }

    public static MqttBrokerBootstrap create(MqttBrokerConfig config) {
        return new MqttBrokerBootstrap(config);
    }

    private MqttBrokerBootstrap(MqttBrokerConfig config) {
        this.config = config;
    }

    /**
     * 自定义netty options
     */
    public MqttBrokerBootstrap serverCustomizer(ServerCustomizer serverCustomizer) {
        this.serverCustomizer = serverCustomizer;
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
        definition.check();
        this.ruleDefinitions.add(definition);
        return this;
    }

    /**
     * 规则链配置
     */
    public MqttBrokerBootstrap rules(RuleDefinition... definitions) {
        return rules(Arrays.asList(definitions));
    }

    /**
     * 规则链配置
     */
    public MqttBrokerBootstrap rules(Collection<RuleDefinition> definitions) {
        definitions.forEach(RuleDefinition::check);
        this.ruleDefinitions.addAll(definitions);
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
    @SuppressWarnings("rawtypes")
    public MqttBrokerBootstrap eventConsumers(MqttEventConsumer... consumers) {
        this.eventConsumers.addAll(Arrays.asList(consumers));
        return this;
    }

    /**
     * 事件consumer
     * 1. {@link EventConsumer}实现类
     * 2. 带{@link org.kin.framework.event.EventFunction}的实例
     */
    @SuppressWarnings("rawtypes")
    public MqttBrokerBootstrap eventConsumers(Collection<MqttEventConsumer> consumers) {
        this.eventConsumers.addAll(consumers);
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
     * 桥接配置
     */
    public MqttBrokerBootstrap bridge(BridgeConfiguration config) {
        config.check();
        this.bridgeConfigurations.add(config);
        return this;
    }

    /**
     * 桥接配置
     */
    public MqttBrokerBootstrap bridges(BridgeConfiguration... configs) {
        return bridges(Arrays.asList(configs));
    }

    /**
     * 桥接配置
     */
    public MqttBrokerBootstrap bridges(Collection<BridgeConfiguration> configs) {
        configs.forEach(BridgeConfiguration::check);
        this.bridgeConfigurations.addAll(configs);
        return this;
    }

    /**
     * start mqtt server及其admin server
     */
    public MqttBroker start() {
        config.selfCheck();
        check();

        MqttBrokerContext brokerContext = new MqttBrokerContext(config, new MqttMessageDispatcher(interceptors),
                authService, messageStore, aclService, shareSubLoadBalance, eventConsumers);

        //初始化cluster component和创建bridges
        return brokerContext.getCluster().init()
                .then(Mono.fromRunnable(() -> {
                    //加载rule定义并apply rule
                    brokerContext.getRuleManager().init(ruleDefinitions);
                    //加载bridge配置并apply bridge
                    brokerContext.getBridgeManager().init(bridgeConfigurations);
                }))
                //初始化mqtt broker
                .then(Mono.fromCallable(() -> initMqttBroker(brokerContext)))
                .block();
    }

    /**
     * 内部配置检查
     */
    private void check() {
        //检查bridge name是否会重复
        Set<String> bridgeNames = new HashSet<>();
        for (BridgeConfiguration bc : bridgeConfigurations) {
            String name = bc.getName();
            if (!bridgeNames.add(name)) {
                throw new MqttBrokerException(String.format("duplicate bridge name '%s'", name));
            }
        }

        if (config.isSsl()) {
            Preconditions.checkArgument(StringUtils.isNotBlank(config.getCertFile()), "ssl is opened, but certFile is not set");
            Preconditions.checkArgument(StringUtils.isNotBlank(config.getCertKeyFile()), "ssl is opened, but certKeyFile is not set");
        }
    }

    /**
     * 初始化mqtt broker
     *
     * @param brokerContext mqtt broker context
     * @return {@link  MqttBroker}
     */
    @SuppressWarnings("rawtypes")
    private MqttBroker initMqttBroker(MqttBrokerContext brokerContext) {
        int port = config.getPort();
        //启动mqtt broker
        LoopResources loopResources = LoopResources.create("kin-mqtt-server-" + port, 2, SysUtils.DOUBLE_CPU, false);
        List<Mono<DisposableServer>> disposableServerMonoList = new LinkedList<>();
        //tcp
        TcpServer tcpServer = TcpServer.create();
        if (config.isSsl()) {
            tcpServer = tcpServer.secure(this::onServerSsl);
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
                .doOnChannelInit(MqttChannelInitializer.create(config))
                .doOnConnection(connection -> {
                    initPreHandlers(connection);
                    initPostHandlers(connection);
                    onMqttClientConnected(brokerContext, new MqttSession(brokerContext, connection));
                });

        Map<ChannelOption, Object> options = Collections.emptyMap();
        Map<ChannelOption, Object> childOptions = Collections.emptyMap();
        if (Objects.nonNull(serverCustomizer)) {
            options = serverCustomizer.options();
            childOptions = serverCustomizer.childOptions();
        }

        tcpServer = applyOptions(tcpServer, options);
        tcpServer = applyOptions(tcpServer, childOptions);

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
            if (config.isSsl()) {
                wsServer = wsServer.secure(this::onServerSsl);
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
                    .doOnChannelInit(MqttChannelInitializer.create(config))
                    .doOnConnection(connection -> {
                        initPreHandlers(connection);

                        //websocket相关
                        connection.addHandlerLast(new HttpServerCodec())
                                .addHandlerLast(new HttpObjectAggregator(65536))
                                .addHandlerLast(new WebSocketServerProtocolHandler(config.getWsPath(), "mqtt, mqttv3.1, mqttv3.1.1"))
                                .addHandlerLast(new WsFrame2ByteBufDecoder())
                                .addHandlerLast(new ByteBuf2WsFrameEncoder());

                        initPostHandlers(connection);
                        onMqttClientConnected(brokerContext, new MqttSession(brokerContext, connection));
                    });

            wsServer = applyOptions(wsServer, options);
            wsServer = applyOptions(wsServer, childOptions);

            disposableServerMono = wsServer.bind()
                    .doOnNext(d -> {
                        //定义mqtt broker over websocket close逻辑
                        d.onDispose(() -> log.info("mqtt broker over websocket(port:{}) closed", wsPort));
                    })
                    .doOnSuccess(d -> log.info("mqtt broker over websocket started on port({})", wsPort))
                    .cast(DisposableServer.class);
            disposableServerMonoList.add(disposableServerMono);
        }

        //初始化sys topic publisher
        if (config.isEnableSysTopic()) {
            initSysTopicPublisher(brokerContext);
        }

        return new MqttBroker(brokerContext, disposableServerMonoList, () -> {
            loopResources.dispose();
            brokerContext.close();
        });
    }

    /**
     * 构建server端ssl上下文
     */
    private void onServerSsl(SslProvider.SslContextSpec sslContextSpec) {
        try {
            SslContextBuilder sslContextBuilder;
            String certFile = config.getCertFile();
            String certKeyFile = config.getCertKeyFile();
            String certKeyPassword = config.getCertKeyPassword();
            if (Objects.nonNull(certFile) && Objects.nonNull(certKeyFile)) {
                //配置证书和私钥
                sslContextBuilder = SslContextBuilder.forServer(new File(certFile), new File(certKeyFile), certKeyPassword);
            } else {
                //自签名证书, for test
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslContextBuilder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
            }
            sslContextBuilder.protocols(PROTOCOLS)
                    .sslProvider(getSslProvider())
                    .clientAuth(ClientAuth.REQUIRE);
            sslContextSpec.sslContext(sslContextBuilder.build());
        } catch (SSLException | CertificateException e) {
            ExceptionUtils.throwExt(e);
        }
    }

    /**
     * 获取ssl provider, 如果支持openssl, 则使用, 否则回退到使用jdk ssl
     */
    private static io.netty.handler.ssl.SslProvider getSslProvider() {
        if (OpenSsl.isAvailable()) {
            return io.netty.handler.ssl.SslProvider.OPENSSL_REFCNT;
        } else {
            return io.netty.handler.ssl.SslProvider.JDK;
        }
    }

    /**
     * 应用netty options
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private <V extends reactor.netty.transport.Transport<?, ?>> V applyOptions(V transport, Map<ChannelOption, Object> options) {
        for (Map.Entry<ChannelOption, Object> entry : options.entrySet()) {
            transport = (V) transport.option(entry.getKey(), entry.getValue());
        }

        return transport;
    }

    /**
     * 初始化前置netty channel handler
     *
     * @param connection 连接
     */
    private void initPreHandlers(Connection connection) {
        int connBytesPerSec = config.getConnBytesPerSec();
        if (connBytesPerSec > 0) {
            //流量整形
            connection.addHandlerLast(new ChannelTrafficShapingHandler(0, connBytesPerSec));
        }
    }

    /**
     * 初始化后置netty channel handler
     *
     * @param connection 连接
     */
    private void initPostHandlers(Connection connection) {
        //mqtt decoder encoder
        connection.addHandlerLast(new MqttDecoder(config.getMessageMaxSize()))
                .addHandlerLast(MqttEncoder.INSTANCE)
                .addHandlerLast(MqttBrokerHandler.DEFAULT);
    }

    /**
     * mqtt client建立连接时触发, mqtt session配置以及处理mqtt消息逻辑
     */
    private void onMqttClientConnected(MqttBrokerContext brokerContext, MqttSession mqttSession) {
        mqttSession.deferCloseWithoutConnMsg()
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
                .flatMap(mqttMessage -> brokerContext.getDispatcher()
                        .dispatch(MqttMessageContext.common(mqttMessage, brokerContext.getBrokerId(),
                                mqttSession.getClientId()), mqttSession, brokerContext))
                .subscribe();
    }

    /**
     * 初始化 sys topic publisher
     */
    private void initSysTopicPublisher(MqttBrokerContext brokerContext) {
        new OnlineClientNumPublisher(brokerContext);
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

    public List<RuleDefinition> getRuleDefinitions() {
        return ruleDefinitions;
    }

    public AclService getAclService() {
        return aclService;
    }

    @SuppressWarnings("rawtypes")
    public List<MqttEventConsumer> getEventConsumers() {
        return eventConsumers;
    }

    public ShareSubLoadBalance getShareSubLoadBalance() {
        return shareSubLoadBalance;
    }
}
