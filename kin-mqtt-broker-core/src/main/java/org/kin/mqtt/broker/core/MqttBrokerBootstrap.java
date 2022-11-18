package org.kin.mqtt.broker.core;

import com.google.common.base.Preconditions;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.kin.framework.utils.LazyInstantiation;
import org.kin.framework.utils.SysUtils;
import org.kin.mqtt.broker.core.auth.NoneAuthService;
import org.kin.mqtt.broker.core.cluster.BrokerManager;
import org.kin.mqtt.broker.core.cluster.ClusterConfig;
import org.kin.mqtt.broker.core.cluster.gossip.GossipBrokerManager;
import org.kin.mqtt.broker.core.cluster.gossip.GossipConfig;
import org.kin.mqtt.broker.core.cluster.standalone.StandaloneBrokerManager;
import org.kin.mqtt.broker.core.message.MqttMessageWrapper;
import org.kin.mqtt.broker.core.store.MemoryMessageStore;
import org.kin.transport.netty.ServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
    /** admin manager port, default 9000 */
    private int adminPort = 9000;
    /** 最大消息大小, 默认4MB */
    private int messageMaxSize = 4194304;
    /** 注册的interceptor */
    private final List<Interceptor> interceptors = new ArrayList<>();
    /** broker manager实例化逻辑 */
    private LazyInstantiation<BrokerManager> brokerManagerInstantiation;
    /** mqtt broker集群配置 */
    private ClusterConfig clusterConfig;

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
     * 定义mqtt server admin port
     */
    public MqttBrokerBootstrap adminPort(int adminPort) {
        Preconditions.checkArgument(port > 0, "adminPort must be greater than 0");
        this.adminPort = adminPort;
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
     * 集群配置
     */
    public MqttBrokerBootstrap cluster(Class<? extends BrokerManager> brokerManagerClass, ClusterConfig clusterConfig) {
        Preconditions.checkNotNull(brokerManagerClass);
        Preconditions.checkNotNull(clusterConfig);
        this.brokerManagerInstantiation = new LazyInstantiation<BrokerManager>(brokerManagerClass) {
        };
        this.clusterConfig = clusterConfig;
        return this;
    }

    /**
     * gossip集群配置
     */
    public MqttBrokerBootstrap gossipCluster(GossipConfig gossipConfig) {
        return cluster(GossipBrokerManager.class, gossipConfig);
    }

    /**
     * start mqtt server及其admin server
     */
    public MqttBroker start() {
        TcpServer tcpServer = TcpServer.create();
        if (isSsl()) {
            tcpServer = tcpServer.secure(this::secure);
        }

        MqttBrokerContext brokerContext = new MqttBrokerContext(port);
        brokerContext.setDispatcher(new MqttMessageDispatcher(interceptors));
        brokerContext.setAuthService(NoneAuthService.INSTANCE);
        brokerContext.setMessageStore(new MemoryMessageStore());
        BrokerManager brokerManager;
        if (Objects.isNull(brokerManagerInstantiation)) {
            brokerManager = StandaloneBrokerManager.INSTANCE;
        } else {
            brokerManager = brokerManagerInstantiation.instance();
        }
        brokerContext.setBrokerManager(brokerManager);

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

        // TODO: 2022/11/12 admin http server
        return new MqttBroker(disposableServerMono);
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
        BrokerManager brokerManager;
        if (Objects.isNull(brokerManagerInstantiation)) {
            brokerManager = StandaloneBrokerManager.INSTANCE;
        } else {
            brokerManager = brokerManagerInstantiation.instance();
        }
        brokerManager.init(this)
                .then(Mono.fromRunnable(() -> brokerManager.clusterMqttMessages()
                        .onErrorResume(e -> Mono.empty())
                        .publishOn(brokerContext.getMqttMessageHandleScheduler())
                        .subscribe(clusterMessage -> brokerContext.getDispatcher().dispatch(
                                        MqttMessageWrapper.fromCluster(clusterMessage),
                                        new MqttBrokerChannel(brokerContext, clusterMessage.getClientId()),
                                        brokerContext),
                                t -> log.error("broker manager handle cluster message error", t))))
                .subscribe();
    }

    //getter
    public int getPort() {
        return port;
    }

    public int getAdminPort() {
        return adminPort;
    }

    public int getMessageMaxSize() {
        return messageMaxSize;
    }

    public List<Interceptor> getInterceptors() {
        return interceptors;
    }

    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }
    // TODO: 2022/11/15 getter
}
