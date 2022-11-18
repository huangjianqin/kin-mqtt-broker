package org.kin.mqtt.broker.core.cluster.gossip;

import com.google.common.base.Preconditions;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.codec.jackson.JacksonMessageCodec;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.reactor.RetryNonSerializedEmitFailureHandler;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import org.kin.framework.utils.NetUtils;
import org.kin.mqtt.broker.core.MqttBrokerBootstrap;
import org.kin.mqtt.broker.core.cluster.BrokerManager;
import org.kin.mqtt.broker.core.cluster.ClusterConfig;
import org.kin.mqtt.broker.core.cluster.MqttBrokerNode;
import org.kin.mqtt.broker.core.message.MqttMessageReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 基于gossip集群发现机制
 *
 * @author huangjianqin
 * @date 2022/11/16
 */
public final class GossipBrokerManager implements BrokerManager {
    private static final Logger log = LoggerFactory.getLogger(GossipBrokerManager.class);

    /** 来自集群广播的mqtt消息流 */
    private final Sinks.Many<MqttMessageReplica> clusterMqttMessageSink = Sinks.many().multicast().onBackpressureBuffer();
    /** gossip cluster */
    private volatile Mono<Cluster> clusterMono;

    @Override
    public Mono<Void> init(MqttBrokerBootstrap bootstrap) {
        ClusterConfig config = bootstrap.getClusterConfig();
        Preconditions.checkNotNull(config);
        Preconditions.checkArgument(config instanceof GossipConfig, "cluster config must be GossipConfig");

        GossipConfig gossipConfig = (GossipConfig) config;

        int port = gossipConfig.getPort();
        clusterMono = new ClusterImpl().config(clusterConfig -> clusterConfig.externalHost(NetUtils.getIp()).externalPort(port))
                .membership(membershipConfig -> membershipConfig.seedMembers(seedMembers(gossipConfig.getSeeds()))
                        .namespace(gossipConfig.getNamespace())
                        .syncInterval(5_000))
                .transport(transportConfig -> transportConfig.transportFactory(new TcpTransportFactory())
                        .messageCodec(JacksonMessageCodec.INSTANCE)
                        .port(port))
                .handler(c -> new GossipMessageHandler())
                .start();
        return clusterMono.then();
    }

    /**
     * gossip member host
     */
    private List<Address> seedMembers(String seeds) {
        //解析seeds
        return Stream.of(seeds.split(";"))
                .map(hostPort -> {
                    String[] splits = hostPort.split(":");
                    return Address.create(splits[0], Integer.parseInt(splits[1]));
                })
                .collect(Collectors.toList());
    }

    @Override
    public Flux<MqttMessageReplica> clusterMqttMessages() {
        return clusterMqttMessageSink.asFlux();
    }

    @Override
    public Flux<MqttBrokerNode> getClusterBrokerNodes() {
        return clusterMono.flatMapIterable(Cluster::members)
                .map(this::clusterNode);
    }

    /**
     * 转换为集群broker信息
     *
     * @param member 集群broker信息
     * @return {@link MqttBrokerNode}
     */
    private MqttBrokerNode clusterNode(Member member) {
        return MqttBrokerNode.builder()
                .name(member.alias())
                .host(member.address().host())
                .port(member.address().port())
                .namespace(member.namespace())
                .build();
    }

    @Override
    public Mono<Void> broadcastMqttMessage(MqttMessageReplica message) {
        return clusterMono.flatMap(c -> {
                    log.debug("cluster broadcast message {} ", message);
                    return c.spreadGossip(Message.withData(message).build());
                })
                .then();
    }

    @Override
    public Mono<Void> shutdown() {
        return Mono.fromRunnable(() -> {
            clusterMono.subscribe(Cluster::shutdown);
            //close sink
            clusterMqttMessageSink.emitComplete(new RetryNonSerializedEmitFailureHandler());
        });
    }

    //------------------------------------------------------------------------------------------------------------------------

    /**
     * gossip消息处理
     */
    private class GossipMessageHandler implements ClusterMessageHandler {

        @Override
        public void onMessage(Message message) {
            log.warn("cluster is not support to handle message {} ", message);
        }

        @Override
        public void onGossip(Message message) {
            log.debug("cluster accept message {} ", message);
            clusterMqttMessageSink.emitNext(message.data(), new RetryNonSerializedEmitFailureHandler());
        }

        @Override
        public void onMembershipEvent(MembershipEvent event) {
            // TODO: 2022/11/16
            Member member = event.member();
            StringJoiner sj = new StringJoiner(":");
            sj.add(member.namespace());
            sj.add(member.alias());
            sj.add(member.address().toString());
            log.info("mqtt broker(namespace:alias:address) '{}' {}", sj, event.type());

            switch (event.type()) {
                case ADDED:
//                    eventMany.tryEmitNext(ClusterStatus.ADDED);
                    break;
                case LEAVING:
//                    eventMany.tryEmitNext(ClusterStatus.LEAVING);
                    break;
                case REMOVED:
//                    eventMany.tryEmitNext(ClusterStatus.REMOVED);
                    break;
                case UPDATED:
//                    eventMany.tryEmitNext(ClusterStatus.UPDATED);
                    break;
                default:
                    break;
            }
        }
    }
}
